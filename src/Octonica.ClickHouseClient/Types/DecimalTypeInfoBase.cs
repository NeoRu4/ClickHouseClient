﻿#region License Apache 2.0
/* Copyright 2019-2020 Octonica
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#endregion

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;

namespace Octonica.ClickHouseClient.Types
{
    internal abstract class DecimalTypeInfoBase : IClickHouseColumnTypeInfo
    {
        public const byte DefaultPrecision = 38, DefaultScale = 9;

        private readonly int? _precision;
        private readonly int? _scale;

        public string ComplexTypeName { get; }

        public string TypeName { get; }

        public int GenericArgumentsCount => 0;

        protected DecimalTypeInfoBase(string typeName, int? precision)
        {
            _precision = precision;
            TypeName = typeName;
            ComplexTypeName = typeName;
        }

        protected DecimalTypeInfoBase(string typeName, string complexTypeName, int precision, int scale)
        {
            if (precision < 1 || precision > 38)
                throw new ArgumentOutOfRangeException(nameof(precision), "The precision must be in the range [1:38].");
            if (scale < 0)
                throw new ArgumentOutOfRangeException(nameof(scale), "The scale must be a non-negative number.");
            if (scale > precision)
                throw new ArgumentOutOfRangeException(nameof(scale), "The scale must not be greater than the precision.");

            TypeName = typeName;
            ComplexTypeName = complexTypeName;
            _precision = precision;
            _scale = scale;
        }

        public IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            if (_precision == null || _scale == null)
            {
                if (_precision == null && _scale == null)
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"Both scale and precision are required for the type \"{TypeName}\".");

                if (_scale == null)
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"Scale is required for the type \"{TypeName}\".");

                // Currently there is no implementation which requires only the precision value
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"Precision is required for the type \"{TypeName}\".");
            }

            return new DecimalReader(_precision.Value, _scale.Value, rowCount);
        }

        public IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (_precision == null && _scale == null)
            {
                var specifiedType = CloneWithOptions(string.Format(CultureInfo.InvariantCulture, "Decimal128({0})", DefaultScale), DefaultPrecision, DefaultScale);
                return specifiedType.CreateColumnWriter(columnName, rows, columnSettings);
            }

            if (_scale == null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"Scale is required for the type \"{TypeName}\".");
            
            if(_precision==null)
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotFullySpecified, $"Precision is required for the type \"{TypeName}\".");

            if (!(rows is IReadOnlyList<decimal> decimalRows))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return new DecimalWriter(columnName, ComplexTypeName, _precision.Value, _scale.Value, decimalRows);
        }

        public IClickHouseColumnTypeInfo GetDetailedTypeInfo(List<ReadOnlyMemory<char>> options, IClickHouseTypeInfoProvider typeInfoProvider)
        {
            int? precision = null;
            int scale;
            if (options.Count == 1)
            {
                if (!int.TryParse(options[0].Span, NumberStyles.Integer, CultureInfo.InvariantCulture, out scale) || scale < 0)
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The scale value for the type \"{TypeName}\" must be a non-negative number.");
            }
            else if (options.Count == 2)
            {
                if (!int.TryParse(options[0].Span, NumberStyles.Integer, CultureInfo.InvariantCulture, out var firstValue) || firstValue <= 0)
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The first parameter in options (precision) for the type \"{TypeName}\" must be a positive number.");

                precision = firstValue;

                if (!int.TryParse(options[1].Span, NumberStyles.Integer, CultureInfo.InvariantCulture, out scale) || scale < 0)
                    throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The second parameter in options (scale) for the type \"{TypeName}\" must be a non-negative number.");
            }
            else
            {
                throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"Too many options for the type \"{TypeName}\".");
            }

            if (_precision != null && precision != null)
                throw new ClickHouseException(ClickHouseErrorCodes.InvalidTypeName, $"The value of the precision can not be redefined for the type \"{TypeName}\".");

            var complexTypeName = TypeName + "(" + string.Join(", ", options) + ")";
            return CloneWithOptions(complexTypeName, precision, scale);
        }

        public Type GetFieldType()
        {
            return typeof(decimal);
        }

        public ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Decimal;
        }

        public IClickHouseTypeInfo GetGenericArgument(int index)
        {
            throw new NotSupportedException($"The type \"{TypeName}\" doesn't have generic arguments.");
        }

        protected abstract DecimalTypeInfoBase CloneWithOptions(string complexTypeName, int? precision, int scale);

        private static int GetElementSize(int precision)
        {
            if (precision <= 9)
                return 4;
            if (precision <= 18)
                return 8;

            Debug.Assert(precision <= 38);
            return 16;
        }

        private sealed class DecimalReader : IClickHouseColumnReader
        {
            private readonly int _rowCount;
            private readonly int _elementSize;
            private readonly byte _scale;

            private readonly uint[] _values;

            private int _position;

            public DecimalReader(int precision, int scale, int rowCount)
            {
                _rowCount = rowCount;
                _elementSize = GetElementSize(precision);
                _values = new uint[_elementSize / 4 * rowCount];
                _scale = (byte) scale;
            }

            public SequenceSize ReadNext(ReadOnlySequence<byte> sequence)
            {
                var elementPosition = _position * sizeof(uint) / _elementSize;
                if (elementPosition >= _rowCount)
                    throw new ClickHouseException(ClickHouseErrorCodes.DataReaderError, "Internal error. Attempt to read after the end of the column.");

                var byteLength = (int) Math.Min((_rowCount - elementPosition) * _elementSize, sequence.Length - sequence.Length % _elementSize);

                Span<byte> tmpSpan = stackalloc byte[sizeof(uint)];
                for (var slice = sequence.Slice(0, byteLength); !slice.IsEmpty; slice = slice.Slice(sizeof(uint)))
                {
                    if (slice.FirstSpan.Length >= sizeof(uint))
                        _values[_position++] = BitConverter.ToUInt32(slice.FirstSpan);
                    else
                    {
                        slice.Slice(0, sizeof(uint)).CopyTo(tmpSpan);
                        _values[_position++] = BitConverter.ToUInt32(tmpSpan);
                    }
                }

                return new SequenceSize(byteLength, byteLength / _elementSize);
            }

            public SequenceSize Skip(ReadOnlySequence<byte> sequence, int maxElementsCount, ref object? skipContext)
            {
                var count = Math.Min(maxElementsCount, (int) sequence.Length / _elementSize);
                return new SequenceSize(count * _elementSize, count);
            }

            public IClickHouseTableColumn EndRead(ClickHouseColumnSettings? settings)
            {
                return EndReadInternal();
            }

            private DecimalTableColumn EndReadInternal()
            {
                var memory = new ReadOnlyMemory<uint>(_values, 0, _position);
                return new DecimalTableColumn(memory, _elementSize / 4, _scale);
            }
        }

        private sealed class DecimalWriter : StructureWriterBase<decimal>
        {
            private const byte MaxDecimalScale = 28;
            private static readonly uint[] Scales = { 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000 };

            private readonly byte _scale;
            
            public DecimalWriter(string columnName, string columnType, int precision, int scale, IReadOnlyList<decimal> rows)
                : base(columnName, columnType, GetElementSize(precision), rows)
            {
                _scale = (byte) scale;
            }

            protected override void WriteElement(Span<byte> writeTo, in decimal value)
            {
                var rescaledValue = Math.Round(value, Math.Min(_scale, MaxDecimalScale), MidpointRounding.AwayFromZero);
                var bits = decimal.GetBits(rescaledValue);

                uint lowLow = unchecked((uint) bits[0]);
                uint lowHigh = unchecked((uint) bits[1]);
                uint highLow = unchecked((uint) bits[2]);
                uint highHigh = 0;

                bool isNegative = (bits[3] & int.MinValue) != 0;
                int scale = (bits[3] & ~int.MinValue) >> 16;

                var deltaScale = _scale - scale;
                if (deltaScale < 0)
                    throw new InvalidOperationException("Internal error: unexpected scale difference.");

                bool overflow = false;
                while (deltaScale > 0)
                {
                    var iterationScale = Math.Min(deltaScale, Scales.Length);
                    deltaScale -= iterationScale;
                    var multiplier = (ulong) Scales[iterationScale - 1];

                    ulong lowLowMul = lowLow * multiplier;
                    ulong lowHighMul = lowHigh * multiplier;
                    ulong highLowMul = highLow * multiplier;
                    ulong highHighMul = highHigh * multiplier;

                    lowLow = unchecked((uint) lowLowMul);
                    lowHigh = unchecked((uint) lowHighMul);
                    highLow = unchecked((uint) highLowMul);
                    highHigh = unchecked((uint) highHighMul);

                    var val = lowLowMul >> 32;
                    if (val != 0)
                    {
                        val += lowHigh;
                        lowHigh = unchecked((uint) val);

                        val >>= 32;
                        if (val != 0)
                        {
                            val += highLow;
                            highLow = unchecked((uint) val);

                            val >>= 32;
                            if (val != 0)
                            {
                                val += highHigh;
                                highHigh = unchecked((uint) val);

                                val >>= 32;
                                if (val != 0)
                                {
                                    overflow = true;
                                    break;
                                }
                            }
                        }
                    }

                    val = lowHighMul >> 32;
                    if (val != 0)
                    {
                        val += highLow;
                        highLow = unchecked((uint)val);

                        val >>= 32;
                        if (val != 0)
                        {
                            val += highHigh;
                            highHigh = unchecked((uint)val);

                            val >>= 32;
                            if (val != 0)
                            {
                                overflow = true;
                                break;
                            }
                        }
                    }

                    val = highLowMul >> 32;
                    if (val != 0)
                    {
                        val += highHigh;
                        highHigh = unchecked((uint)val);

                        val >>= 32;
                        if (val != 0)
                        {
                            overflow = true;
                            break;
                        }
                    }

                    val = highHighMul >> 32;
                    if (val != 0)
                    {
                        overflow = true;
                        break;
                    }
                }

                if (!overflow)
                {
                    if (isNegative)
                    {
                        lowLow = unchecked(0 - lowLow);
                        uint max = lowLow == 0 ? 0 : uint.MaxValue;

                        lowHigh = unchecked(max - lowHigh);
                        if (lowHigh != 0 && max == 0)
                            max = uint.MaxValue;

                        highLow = unchecked(max - highLow);
                        if (highLow != 0 && max == 0)
                            max = uint.MaxValue;

                        highHigh = unchecked(max - highHigh);

                        if (ElementSize == 4)
                            overflow = highHigh != uint.MaxValue || highLow != uint.MaxValue || lowHigh != uint.MaxValue || (lowLow & unchecked((uint) int.MinValue)) == 0;
                        else if (ElementSize == 8)
                            overflow = highHigh != uint.MaxValue || highLow != uint.MaxValue || (lowHigh & unchecked((uint) int.MinValue)) == 0;
                        else
                            overflow = (highHigh & unchecked((uint) int.MinValue)) == 0;

                        if (overflow && rescaledValue == 0)
                            overflow = false;
                    }
                    else
                    {
                        if (ElementSize == 4)
                            overflow = highHigh != 0 || highLow != 0 || lowHigh != 0 || (lowLow & unchecked((uint) int.MinValue)) != 0;
                        else if (ElementSize == 8)
                            overflow = highHigh != 0 || highLow != 0 || (lowHigh & unchecked((uint) int.MinValue)) != 0;
                        else
                            overflow = (highHigh & unchecked((uint) int.MinValue)) != 0;
                    }
                }

                if (overflow)
                    throw new OverflowException($"The decimal value is too big and can't be written to the column of type \"{ColumnType}\".");

                var success = BitConverter.TryWriteBytes(writeTo, lowLow);
                Debug.Assert(success);
                if (ElementSize == 4)
                    return;

                success = BitConverter.TryWriteBytes(writeTo.Slice(4), lowHigh);
                Debug.Assert(success);
                if (ElementSize == 8)
                    return;

                Debug.Assert(ElementSize == 16);
                success = BitConverter.TryWriteBytes(writeTo.Slice(8), highLow);
                Debug.Assert(success);
                success = BitConverter.TryWriteBytes(writeTo.Slice(12), highHigh);
                Debug.Assert(success);
            }
        }
    }
}

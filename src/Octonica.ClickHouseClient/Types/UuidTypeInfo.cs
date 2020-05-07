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
using System.Collections.Generic;
using System.Diagnostics;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class UuidTypeInfo : SimpleTypeInfo
    {
        public UuidTypeInfo()
            : base("UUID")
        {
        }

        public override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new UuidReader(rowCount);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            if (!(rows is IReadOnlyList<Guid> guidRows))
                throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");

            return new UuidWriter(columnName, ComplexTypeName, guidRows);
        }

        public override Type GetFieldType()
        {
            return typeof(Guid);
        }

        public override ClickHouseDbType GetDbType()
        {
            return ClickHouseDbType.Guid;
        }

        private sealed class UuidReader : StructureReaderBase<Guid>
        {
            public UuidReader(int rowCount)
                : base(16, rowCount)
            {
            }

            protected override Guid ReadElement(ReadOnlySpan<byte> source)
            {
                ushort c = BitConverter.ToUInt16(source.Slice(0));
                ushort b = BitConverter.ToUInt16(source.Slice(2));
                uint a = BitConverter.ToUInt32(source.Slice(4));
                
                return new Guid(a, b, c, source[15], source[14], source[13], source[12], source[11], source[10], source[9], source[8]);
            }
        }

        private sealed class UuidWriter:StructureWriterBase<Guid>
        {
            public UuidWriter(string columnName, string columnType, IReadOnlyList<Guid> rows)
                : base(columnName, columnType, 16, rows)
            {
            }

            protected override void WriteElement(Span<byte> writeTo, in Guid value)
            {
                var success = value.TryWriteBytes(writeTo);
                Debug.Assert(success);

                for (int i = 0; i < 4; i++)
                {
                    var tmp = writeTo[i];
                    writeTo[i] = writeTo[7 - i];
                    writeTo[7 - i] = tmp;

                    tmp = writeTo[8 + i];
                    writeTo[8 + i] = writeTo[15 - i];
                    writeTo[15 - i] = tmp;
                }
            }
        }
    }
}

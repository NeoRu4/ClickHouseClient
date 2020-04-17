﻿#region License Apache 2.0
/* Copyright 2020 Octonica
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
using System.Net;
using System.Net.Sockets;
using Octonica.ClickHouseClient.Exceptions;
using Octonica.ClickHouseClient.Protocol;
using Octonica.ClickHouseClient.Utils;

namespace Octonica.ClickHouseClient.Types
{
    internal sealed class IpV6TypeInfo: SimpleTypeInfo
    {
        private const int AddressSize = 16;

        public IpV6TypeInfo()
            : base("IPv6")
        {
        }

        public override IClickHouseColumnReader CreateColumnReader(int rowCount)
        {
            return new IpV6Reader(rowCount);
        }

        public override IClickHouseColumnWriter CreateColumnWriter<T>(string columnName, IReadOnlyList<T> rows, ClickHouseColumnSettings? columnSettings)
        {
            IReadOnlyList<IPAddress?> preparedRows;
            switch (rows)
            {
                case IReadOnlyList<IPAddress?> ipAddressRows:
                    preparedRows = ipAddressRows;
                    break;
                case IReadOnlyList<string?> stringRows:
                    preparedRows = new MappedReadOnlyList<string?, IPAddress?>(stringRows, ParseIpAddress);
                    break;
                default:
                    throw new ClickHouseException(ClickHouseErrorCodes.TypeNotSupported, $"The type \"{typeof(T)}\" can't be converted to the ClickHouse type \"{ComplexTypeName}\".");
            }

            return new IpV6Writer(columnName, TypeName, preparedRows);
        }

        public override Type GetFieldType()
        {
            return typeof(IPAddress);
        }

        private static IPAddress? ParseIpAddress(string? address)
        {
            if (address == null)
                return null;

            if (!IPAddress.TryParse(address, out var ipAddress))
                throw new InvalidCastException($"The string \"{address}\" is not a valid IPv4 address.");

            return ipAddress;
        }

        private sealed class IpV6Reader : IpColumnReaderBase
        {
            public IpV6Reader(int rowCount)
                : base(rowCount, AddressSize)
            {
            }

            protected override IClickHouseTableColumn<IPAddress> EndRead(ReadOnlyMemory<byte> buffer)
            {
                return new IpV6TableColumn(buffer);
            }
        }

        private sealed class IpV6Writer : IClickHouseColumnWriter
        {
            private readonly IReadOnlyList<IPAddress?> _rows;

            private int _position;

            public string ColumnName { get; }

            public string ColumnType { get; }

            public IpV6Writer(string columnName, string columnType, IReadOnlyList<IPAddress?> rows)
            {
                _rows = rows;
                ColumnName = columnName;
                ColumnType = columnType;
            }

            public SequenceSize WriteNext(Span<byte> writeTo)
            {
                var elementsCount = Math.Min(_rows.Count - _position, writeTo.Length / AddressSize);

                for (int i = 0; i < elementsCount; i++, _position++)
                {
                    var ipAddress = _rows[i];
                    if (ipAddress == null)
                    {
                        writeTo.Slice(i * AddressSize).Fill(0);
                        continue;
                    }

                    if (ipAddress.AddressFamily == AddressFamily.InterNetwork)
                        ipAddress = ipAddress.MapToIPv6();

                    if (ipAddress.AddressFamily != AddressFamily.InterNetworkV6)
                        throw new InvalidCastException($"The network address \"{ipAddress}\" is not a IPv6 address.");

                    if (!ipAddress.TryWriteBytes(writeTo.Slice(i * AddressSize, AddressSize), out var bytesWritten) || bytesWritten != AddressSize)
                        throw new ClickHouseException(ClickHouseErrorCodes.InternalError, "Internal error: IPv6 address writing error.");
                }

                return new SequenceSize(elementsCount * AddressSize, elementsCount);
            }
        }
    }
}

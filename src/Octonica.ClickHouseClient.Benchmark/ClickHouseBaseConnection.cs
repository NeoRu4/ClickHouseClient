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

using Octonica.ClickHouseClient.Exceptions;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;


namespace Octonica.ClickHouseClient.Benchmark
{
    public class ClickHouseBaseConnection
    {
        private const string ConfigExample = "host=domain.com; port=9000; user=default; password=pw; compress=false;";

        private ClickHouseConnectionSettings? _settings;

        public ClickHouseConnectionSettings GetDefaultConnectionSettings()
        {
            if (_settings != null)
            {
                return _settings;
            }
            var config = this.ReadConfigFile();

            // Compress has bugs with big batch data
            config.Compress = false;

            _settings = config.BuildSettings();
            
            return _settings;
        }

        public async Task<ClickHouseConnection> OpenConnectionAsync(ClickHouseConnectionSettings settings, CancellationToken cancellationToken)
        {
            ClickHouseConnection connection = new ClickHouseConnection(settings);
            await connection.OpenAsync(cancellationToken);

            return connection;
        }

        public async Task<ClickHouseConnection> OpenConnectionAsync()
        {
            return await OpenConnectionAsync(GetDefaultConnectionSettings(), CancellationToken.None);
        }

        public ClickHouseConnection OpenConnection()
        {
            ClickHouseConnection connection = new ClickHouseConnection(GetDefaultConnectionSettings());
            connection.Open();

            return connection;
        }

        private ClickHouseConnectionStringBuilder ReadConfigFile()
        {
            //each benchmark makes new project path, it config file needs to be with Runner path
            string configPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../clickHouse.dbconfig"));

            if (!File.Exists(configPath))
            {
                throw new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, $"Need database connection config: {configPath} \t {ConfigExample}");
            }

            string configText = File.ReadAllText(configPath);

            ClickHouseConnectionStringBuilder builder = new ClickHouseConnectionStringBuilder(configText);
            if (builder.Host == null)
            {
                throw new ClickHouseException(ClickHouseErrorCodes.InvalidConnectionState, $"Example \t {ConfigExample}");
            }

            return builder;
        }

    }

}

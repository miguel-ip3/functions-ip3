using System;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using functions.dto;
using System.Data.SqlClient;
using System.Text.Json;
using System.Threading;
using Microsoft.Extensions.Configuration;
using System.Net.Http;

namespace functions
{
    public class callbackService
    {
        private readonly ILogger _logger;
        // private readonly IConfiguration _configuration;
        // private readonly int _blockSize;
        // private readonly int _pauseMilliseconds;

        public callbackService(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<callbackService>();

            // _configuration = new ConfigurationBuilder()
            //    .SetBasePath(Directory.GetCurrentDirectory())
            //    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            //    .Build();

            // _blockSize = _configuration.GetValue<int>("SendCallbackEmail:BlockSize");
            // _pauseMilliseconds = _configuration.GetValue<int>("SendCallbackEmail:PauseMilliseconds");
        }


        // Conectar com o banco de dados
        public async Task ConnectDB()
        {
            int countEmail = 0;

            Connection dbConnection = new Connection("DatabaseEmail");
            using (SqlConnection connection = dbConnection.GetConnection())
            {
                try
                {
                    // Estabelecendo conexão..
                    connection.Open();
                    Console.WriteLine("Conexão com o banco de dados aberta com sucesso.");

                    // Consulta SQL...
                    string query = @"
                        SELECT
                            E.[LOSE_ID],
                            E.[CLIE_ID],
                            E.[MAIL_Identificador],
                            E.[MAIL_EnderecoEmail],
                            E.[LOSE_StatusRetorno],
                            E.[LOSE_DetalheStatus],
                            E.[LOSE_DataHoraStatus],
                            E.[LOSE_Enviado],
                            E.[LOSE_DataHoraEnvio],
                            W.[WEBHOOK],
                            W.[BLOCK_Size],
                            W.[PAUSE_Milliseconds]
                        FROM 
                            [dbo].[MKT_Log_StatusEnvio] E
                        INNER JOIN 
                            [dbo].[MKT_Webhooks] W ON E.CLIE_ID = W.CLIE_ID
                        WHERE 
                            (E.[LOSE_Enviado] IS NULL OR E.[LOSE_Enviado] = @loseEnviado) AND W.[WEBHOOK_Ativo] = 1
                        ORDER BY 
                            E.[LOSE_DataHoraStatus];";

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@loseEnviado", 0);
                        //command.Parameters.AddWithValue("@callbackEnviado", 0);

                        // Convertendo o resultado da consulta para Dictionary
                        var results = ExecuteQueryDictionary(command, ref countEmail);

                        Console.WriteLine($"============================================================================");
                        Console.WriteLine($"Encontrados um total de {countEmail} registros de Email como status enviado");
                        Console.WriteLine($"============================================================================");

                        // Processando e exibindo os resultados em blocos de 5
                        await SendInBlocksAsync(results, connection);
                    }
                }
                catch (Exception ex)
                {
                    // Erro de conexão..
                    Console.WriteLine("Ocorreu um erro ao abrir a conexão: " + ex.Message);
                }
                finally
                {
                    // Fechando a conexão..
                    connection.Close();
                    Console.WriteLine("Conexão com o banco de dados fechada.");
                }
            }
        }

        private List<Dictionary<string, object>> ExecuteQueryDictionary(SqlCommand command, ref int countEmail)
        {
            var results = new List<Dictionary<string, object>>();

            using (SqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    countEmail++;
                    var row = new Dictionary<string, object>
                    {
                        ["LOSE_ID"] = reader["LOSE_ID"],
                        ["CLIE_ID"] = reader["CLIE_ID"],
                        ["MAIL_Identificador"] = reader["MAIL_Identificador"],
                        ["MAIL_EnderecoEmail"] = reader["MAIL_EnderecoEmail"],
                        ["LOSE_StatusRetorno"] = reader["LOSE_StatusRetorno"],
                        ["LOSE_DetalheStatus"] = reader["LOSE_DetalheStatus"],
                        ["LOSE_DataHoraStatus"] = reader["LOSE_DataHoraStatus"],
                        ["LOSE_Enviado"] = reader["LOSE_Enviado"],
                        ["LOSE_DataHoraEnvio"] = reader["LOSE_DataHoraEnvio"],
                        ["WEBHOOK"] = reader["WEBHOOK"],
                        ["BLOCK_Size"] = reader["BLOCK_Size"],
                        ["PAUSE_Milliseconds"] = reader["PAUSE_Milliseconds"]
                    };
                    results.Add(row);
                }
            }

            return results;
        }




        //Convertendo um resultado de consulta para JSON..


        public async Task SendInBlocksAsync(List<Dictionary<string, object>> results, SqlConnection connection)
        {
            var groupedResultsbyClie = results.GroupBy(r => r["CLIE_ID"]).ToList();

            List<List<Dictionary<string, object>>> blocks = new List<List<Dictionary<string, object>>>();

            foreach (var group in groupedResultsbyClie)
            {
                var groupList = group.ToList();
                int blockSize = (int)groupList.First()["BLOCK_Size"];

                for (int i = 0; i < groupList.Count; i += blockSize)
                {
                    var block = groupList.Skip(i).Take(blockSize).ToList();
                    blocks.Add(block);
                }
            }

            using (var httpClient = new HttpClient())
            {
                foreach (var block in blocks)
                {
                    var clieId = (int)block.First()["CLIE_ID"];
                    string webhook = block.First()["WEBHOOK"].ToString();
                    int blockSize = (int)block.First()["BLOCK_Size"];
                    int pauseMilliseconds = (int)block.First()["PAUSE_Milliseconds"];

                    if (!string.IsNullOrEmpty(webhook))
                    {
                        var filteredBlock = block.Select(record => new
                        {
                            MAIL_Identificador = record["MAIL_Identificador"],
                            MAIL_EnderecoEmail = record["MAIL_EnderecoEmail"],
                            LOSE_StatusRetorno = record["LOSE_StatusRetorno"],
                            LOSE_DetalheStatus = record["LOSE_DetalheStatus"],
                            LOSE_DataHoraStatus = record["LOSE_DataHoraStatus"],
                        }).ToList();

                        string blockJson = JsonSerializer.Serialize(filteredBlock, new JsonSerializerOptions { WriteIndented = true });

                        Console.WriteLine("Enviando bloco de Email:");
                        Console.WriteLine(blockJson);

                        var content = new StringContent(blockJson, System.Text.Encoding.UTF8, "application/json");

                        var response = await httpClient.PostAsync(webhook, content);

                        if (response.IsSuccessStatusCode)
                        {
                            UpdateCallbackEnviado(connection, block);
                        }
                        else
                        {
                            Console.WriteLine($"Falha ao enviar o bloco para o webhook: {webhook}");
                        }

                        if (block != blocks.Last())
                        {
                            Console.WriteLine("=======================================================================================================");
                            Console.WriteLine($"Pausando o bloco de Email por {pauseMilliseconds / 1000} segundos antes do próximo bloco de {blockSize}...");
                            Console.WriteLine("=======================================================================================================");
                            await Task.Delay(pauseMilliseconds);
                        }
                    }
                    else
                    {
                        Console.WriteLine($"Não foi possível obter o webhook ou configuração de Email para o cliente {clieId}. Bloco ignorado.");
                    }
                }
            }
        }

        // Função para obter o webhook do tipo email para um determinado CLIE_ID
        // private (string webhook, int blockSize, int pauseMilliseconds) GetWebhook(SqlConnection connection, int clieId)
        // {
        //     string webhook = string.Empty;
        //     int blockSize = 0;
        //     int pauseMilliseconds = 0;

        //     // Consulta SQL para buscar o webhook, BLOCK_Size e PAUSE_Milliseconds para o CLIE_ID
        //     string query = @"
        //                     SELECT 
        //                         WEBHOOK,
        //                         BLOCK_Size,
        //                         PAUSE_Milliseconds
        //                     FROM 
        //                         [dbo].[MKT_Webhooks] 
        //                     WHERE 
        //                         CLIE_ID = @clieId AND WEBHOOK_Ativo = @ativo";

        //     using (var command = new SqlCommand(query, connection))
        //     {
        //         command.Parameters.AddWithValue("@clieId", clieId);
        //         command.Parameters.AddWithValue("@ativo", 1);

        //         using (var reader = command.ExecuteReader())
        //         {
        //             if (reader.Read())
        //             {
        //                 webhook = reader.GetString(reader.GetOrdinal("WEBHOOK"));
        //                 blockSize = reader.GetInt32(reader.GetOrdinal("BLOCK_Size"));
        //                 pauseMilliseconds = reader.GetInt32(reader.GetOrdinal("PAUSE_Milliseconds"));
        //             }
        //         }
        //     }

        //     return (webhook, blockSize, pauseMilliseconds);
        // }


        private void UpdateCallbackEnviado(SqlConnection connection, List<Dictionary<string, object>> block)
        {
            try
            {
                // Lista para armazenar os IDs para a atualização em massa
                List<int> idsToUpdate = new List<int>();

                foreach (var item in block)
                {
                    // Verificar se a chave "LOSE_ID" está presente no dicionário
                    if (item.ContainsKey("LOSE_ID"))
                    {
                        // Tentar converter o valor associado à chave "LOSE_ID" para inteiro
                        if (int.TryParse(item["LOSE_ID"].ToString(), out int loseId))
                        {
                            idsToUpdate.Add(loseId);
                        }
                        else
                        {
                            Console.WriteLine("O valor associado à chave 'LOSE_ID' não pôde ser convertido para inteiro.");
                        }
                    }
                    else
                    {
                        Console.WriteLine("A chave 'LOSE_ID' não está presente no dicionário.");
                    }
                }

                if (idsToUpdate.Count > 0)
                {
                    // Formar a string de parâmetros para a cláusula IN
                    string idsParameter = string.Join(",", idsToUpdate);

                    // Construir a consulta de atualização em massa
                    string updateQuery = $@"
                                            UPDATE [dbo].[MKT_Log_StatusEnvio] 
                                            SET 
                                                [LOSE_Enviado] = @loseenviado, 
                                                [LOSE_DataHoraEnvio] = @dataHoraEnvio 
                                            WHERE [LOSE_ID] IN ({idsParameter})";

                    using (SqlCommand updateCommand = new SqlCommand(updateQuery, connection))
                    {
                        updateCommand.Parameters.Clear();
                        updateCommand.Parameters.AddWithValue("@loseenviado", 1);
                        updateCommand.Parameters.AddWithValue("@dataHoraEnvio", DateTime.Now);
                        updateCommand.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Ocorreu um erro ao atualizar/enviar os registros: " + ex.Message);
                // Você pode adicionar mais tratamento de erro aqui, se necessário
            }
        }




        [Function("callbackService")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo myTimer)
        {


            _logger.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            await ConnectDB();

            // if (myTimer.ScheduleStatus is not null)
            // {
            //     Console.WriteLine($"-----------------------------------------------------");
            //     _logger.LogInformation($"Next timer schedule at: {myTimer.ScheduleStatus.Next}");
            // }
        }
    }
}

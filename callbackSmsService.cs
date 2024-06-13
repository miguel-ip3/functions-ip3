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
    public class callbackSmsService
    {
        private readonly ILogger _logger;
        private readonly IConfiguration _configuration;
        private readonly int _blockSize;
        private readonly int _pauseMilliseconds;

        public callbackSmsService(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<callbackSmsService>();

            _configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            _blockSize = _configuration.GetValue<int>("SendCallbackSms:BlockSize");
            _pauseMilliseconds = _configuration.GetValue<int>("SendCallbackSms:PauseMilliseconds");
        }

        // Conectar com o banco de dados
        public async Task ConnectDB()
        {
            int countSms = 0;

            Connection dbConnection = new Connection();
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
                                [LOSE_ID],
                                [CLIE_ID],
                                [ENVI_ID],
                                [ENVI_Identificador],
                                [ENVI_StatusCode],
                                [LOSE_StatusRetorno],
                                [LOSE_DataHoraStatus],
                                [LOSE_Enviado],
                                [LOSE_DatatHoraEnvio],
                                [CALLBACK_Enviado]
                            FROM 
                                [ip3Teste].[dbo].[SMS_Log_StatusEnvio]
                            WHERE 
                                [LOSE_Enviado] = @loseEnviado
                                AND ([CALLBACK_Enviado] IS NULL OR [CALLBACK_Enviado] = @callbackEnviado) -- Verifica se CALLBACK_Enviado é NULL ou 0
                            ORDER BY 
                                [LOSE_DataHoraStatus] DESC;";

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@loseEnviado", 1);
                        command.Parameters.AddWithValue("@callbackEnviado", 0);

                        // Convertendo o resultado da consulta para Dictionary
                        var results = ExecuteQueryDictionary(command, ref countSms);

                        Console.WriteLine($"==============================================");
                        Console.WriteLine($"Encontrados um total de {countSms} registros de SMS como status enviado");
                        Console.WriteLine($"==============================================");

                        // Processando e exibindo os resultados em blocos de 5
                        await SendInBlocksAsync(results, _blockSize, _pauseMilliseconds, connection);
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

        private List<Dictionary<string, object>> ExecuteQueryDictionary(SqlCommand command, ref int countSms)
        {
            var results = new List<Dictionary<string, object>>();

            using (SqlDataReader reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    countSms++;
                    var row = new Dictionary<string, object>
                    {
                        ["LOSE_ID"] = reader["LOSE_ID"],
                        ["CLIE_ID"] = reader["CLIE_ID"],
                        ["ENVI_ID"] = reader["ENVI_ID"],
                        ["ENVI_Identificador"] = reader["ENVI_Identificador"],
                        ["ENVI_StatusCode"] = reader["ENVI_StatusCode"],
                        ["LOSE_StatusRetorno"] = reader["LOSE_StatusRetorno"],
                        ["LOSE_DataHoraStatus"] = reader["LOSE_DataHoraStatus"],
                        ["LOSE_Enviado"] = reader["LOSE_Enviado"],
                        ["LOSE_DatatHoraEnvio"] = reader["LOSE_DatatHoraEnvio"],
                        ["CALLBACK_Enviado"] = reader["CALLBACK_Enviado"]
                    };
                    results.Add(row);
                }
            }

            return results;
        }




        //Convertendo um resultado de consulta para JSON..


        public async Task SendInBlocksAsync(List<Dictionary<string, object>> results, int blockSize, int pauseMilliseconds, SqlConnection connection)
        {
            // Agrupando os resultados por CLIE_ID
            var groupedResultsbyClie = results.GroupBy(r => r["CLIE_ID"]).ToList();

            List<List<Dictionary<string, object>>> blocks = new List<List<Dictionary<string, object>>>();

            // Formar blocos apenas se houver um webhook do tipo email associado ao CLIE_ID
            foreach (var group in groupedResultsbyClie)
            {
                var clieId = (int)group.Key; // Obtendo o CLIE_ID do grupo

                // Verificando se há webhooks de email para este CLIE_ID
                var smsWebhook = GetWebhook(connection, clieId, "sms");

                if (!string.IsNullOrEmpty(smsWebhook))
                {
                    var groupList = group.ToList();

                    // Dividindo cada grupo em blocos do tamanho especificado
                    for (int i = 0; i < groupList.Count; i += blockSize)
                    {
                        var block = groupList.Skip(i).Take(blockSize).ToList();
                        blocks.Add(block);
                    }
                }
                else
                {
                    Console.WriteLine($"Não há webhooks de SMS para o cliente {clieId}. Ignorando os resultados deste cliente.");
                }
            }

            // Iterando pelos blocos e exibindo-os com pausa entre eles
            using (var httpClient = new HttpClient())
            {
                foreach (var block in blocks)
                {
                    // Obtendo o webhook do primeiro item do bloco (assumindo que todos os itens do bloco têm o mesmo CLIE_ID)
                    var clieId = (int)block.First()["CLIE_ID"];
                    var smsWebhook = GetWebhook(connection, clieId, "sms");

                    if (!string.IsNullOrEmpty(smsWebhook))
                    {

                        // var filteredBlock = block.Select(record => new
                        // {
                        //     LOSE_ID = record["LOSE_ID"],
                        //     CLIE_ID = record["CLIE_ID"],
                        //     ENVI_ID = record["ENVI_ID"],
                        //     ENVI_Identificador = record["ENVI_Identificador"]
                            
                        // }).ToList();

                        // Retornando para o formato de JSON para envio
                        string blockJson = JsonSerializer.Serialize(block, new JsonSerializerOptions { WriteIndented = true });

                        //Visualizando os blocos enviados..
                        Console.WriteLine("Enviando bloco de SMS:");
                        Console.WriteLine(blockJson);

                        // Criando o conteúdo para a requisição HTTP
                        var content = new StringContent(blockJson, System.Text.Encoding.UTF8, "application/json");

                        // Realizar chamada HTTP POST e aguardar a resposta
                        var response = await httpClient.PostAsync(smsWebhook, content);

                        if (response.IsSuccessStatusCode)
                        {
                            // Atualizar o CALLBACK_Enviado no banco de dados
                            UpdateCallbackEnviado(connection, block);
                        }
                        else
                        {
                            Console.WriteLine($"Falha ao enviar o bloco para o webhook: {smsWebhook}");
                        }

                        // Verificando se não é o último bloco a ser executado
                        if (block != blocks.Last())
                        {
                            Console.WriteLine("========================================");
                            Console.WriteLine($"Pausando o bloco de Email por {pauseMilliseconds / 1000} segundos antes do próximo bloco de {blockSize}...");
                            Console.WriteLine("========================================");
                            await Task.Delay(pauseMilliseconds); // Pausa assíncrona entre blocos
                        }
                    }
                    else
                    {
                        Console.WriteLine($"Não foi possível obter o webhook de sms para o cliente {clieId}. Bloco ignorado.");
                    }
                }
            }
        }

        // Função para obter o webhook do tipo email para um determinado CLIE_ID
        private string GetWebhook(SqlConnection connection, int clieId, string typeWebhook)
        {
            string webhook = string.Empty;

            // Consulta SQL para buscar o webhook para o CLIE_ID e tipo de webhook
            string query = @"SELECT 
                                WEBHOOK 
                            FROM 
                                [ip3Teste].[dbo].[MKT_Webhooks] 
                            WHERE 
                                CLIE_ID = @clieId AND TYPE_WEBHOOK = @typeWebhook";

            using (var command = new SqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@clieId", clieId);
                command.Parameters.AddWithValue("@typeWebhook", typeWebhook);

                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        webhook = reader.GetString(0);
                    }
                }
            }

            return webhook;
        }


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
                    string updateQuery = $"UPDATE [ip3Teste].[dbo].[SMS_Log_StatusEnvio] SET [CALLBACK_Enviado] = @callbackEnviado WHERE [LOSE_ID] IN ({idsParameter})";

                    using (SqlCommand updateCommand = new SqlCommand(updateQuery, connection))
                    {
                        updateCommand.Parameters.Clear();
                        updateCommand.Parameters.AddWithValue("@callbackEnviado", 1);
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

        [Function("callbackSmsService")]
        public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo myTimer)
        {
            // Acessar banco de dados de Adriano

            // Utilizar o pool de conexões para realizar uma query no campo discutido, verificar a última atualização (1 min atrás)

            // Enviar uma requisição com o objeto para o webhook do cliente (enviar via post pro endpoint do cliente. Verificar quantidade de disparos..)

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

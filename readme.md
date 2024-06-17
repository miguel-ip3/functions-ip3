# Conexão com o banco

**Connection.cs - Gerenciamento de Conexão com Banco de Dados**
O arquivo Connection.cs define uma classe responsável por gerenciar as conexões com diferentes bancos de dados, facilitando a troca entre eles com base em uma configuração definida em appsettings.json.

**Como Funciona**
A classe Connection é projetada para abstrair a lógica de construção da string de conexão, permitindo uma fácil seleção do banco de dados a ser utilizado no momento da inicialização.

**Configuração**
O arquivo appsettings.json é utilizado para armazenar as strings de conexão para diferentes bancos de dados. Este arquivo é lido pela classe Connection para construir a connection string adequada.

*Estrutura do appsettings.json executando localmente:*



```json
{
  "ConnectionStrings": {
    "DatabaseEmail": "Server=localhost\\SQLEXPRESS;Database=ip3Teste;Integrated Security=True;",
    "DatabaseSMS": "Server=localhost\\SQLEXPRESS;Database=ip3Teste_SMS;Integrated Security=True;"
  }
}
```



*Exemplo de appsettings.json para um Servidor Real:*


```json
{
  "ConnectionStrings": {
    "DatabaseEmail": "Server=meuServidorSQL;Database=ip3Teste;User Id=meuUsuario;Password=minhaSenha;",
    "DatabaseSMS": "Server=meuServidorSQL;Database=ip3Teste_SMS;User Id=meuUsuario;Password=minhaSenha;"
  }
}

```

**Utilização:**
Nos arquivos callbackService.cs e callbackSmsService.cs inicializar a conexão desta forma respectivamente:
```c#
Connection dbConnection = new Connection("DatabaseEmail");
Connection dbConnection = new Connection("DatabaseSMS");
```

# Tabela do banco de dados

Foram adicionadas mais duas tabelas aos bancos de dados:  
dbo.MKT_Webhooks  
dbo.SMS_Webhooks

Essas tabelas possuem as seguintes colunas:  
- `WEBHOOK_ID [int] IDENTITY(1,1) NOT NULL`: ID da tupla  
- `CLIE_ID`: ID do cliente relacionado ao webhook  
- `WEBHOOK varchar[255]`: O próprio valor do webhook utilizado para enviar os blocos de JSON  
- `BLOCK_Size [int]`: Tamanho dos blocos a serem enviados para o webhook  
- `PAUSE_Milliseconds [int]`: Intervalo entre os envios dos blocos  

# Site de testes

Foi utilizado o site [https://webhook.site/](https://) para fins de teste de funcionalidade para os webhooks.

# Lembrete

Atualizar o campo **LOSE_Enviado** para 1 para evitar envio de mensagens já enviadas.
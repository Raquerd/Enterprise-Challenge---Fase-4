# 💡 Exemplo de Proposta Descritiva – Prevenção de Falhas em Máquina de Solda

## 🛠️ Problema
Máquinas de solda industriais estão sujeitas a falhas inesperadas, como superaquecimento, desgaste de componentes ou problemas elétricos. Essas falhas podem parar a produção, gerar prejuízos e comprometer a segurança.
___
## 🧠 Solução Proposta
Desenvolver um sistema inteligente que monitore em tempo real variáveis como temperatura, corrente elétrica, vibração e tempo de operação da máquina de solda. Com esses dados, será possível identificar padrões que antecedem falhas, emitir alertas automáticos e gerar relatórios preditivos para manutenção preventiva.
___
## 🔧 Tecnologias Envolvidas
| Componente        | Tecnologia                          |
| :---------------- | :---------------------------------- |
| Coleta de Dados	  | ESP32 + sensores físicos            | 
| Transmissão	      | MQTT ou HTTP                        | 
| Armazenamento	    | PostgreSQL (RDS), SQLite (local)    | 
| Processamento   	| Python, Pandas, Scikit-learn        | 
| Modelagem IA	    | Scikit-learn ou TensorFlow          | 
| Visualização	    | Grafana, Streamlit ou Dash          | 
| Hospedagem	      | AWS EC2, AWS S3, ou local           | 
| Comunicação     	| Telegram Bot ou E-mail              | 
___
## 🧱 Arquitetura da Solução (Pipeline de Dados)
1. Coleta de Dados (Sensores IoT):
   
Sensores utilizados:
- Termopar ou sensor de temperatura (TMP36);
- Acelerômetro (MPU-6050);
- Sensor de corrente elétrica (ACS712);
- Contador de ciclos de operação.

Controlador: ESP32 (com Wi-Fi ou MQTT para envio de dados).

2. Transmissão de Dados:
- Envio dos dados via protocolo MQTT ou HTTP para um broker ou API REST.

3. Armazenamento:
- Banco de dados em nuvem  AWS RDS com PostgreSQL
- Alternativa local para testes: SQLite.

4. Processamento de Dados:
- Servidor de processamento por exemplo: AWS EC2 ou computador local com Python;
- Pipeline com Pandas e Scikit-learn para análise de dados e modelagem preditiva;
- Identificação de padrões de falha com Machine Learning supervisionado.

5. Visualização e Alertas:
- Dashboard interativo com Grafana ou Streamlit;
- Alertas por e-mail/Telegram em casos de anomalia;
- Relatórios automáticos com histórico de desempenho e recomendações de manutenção.

6. Armazenamento dos Modelos e Logs:
- Modelos treinados salvos em bucket na AWS S3;
- Logs e resultados também podem ser armazenados em CloudWatch ou banco de logs.
___
## 📦 Estratégia de Dados
**Coleta simulada:** 

Nesta fase utilizaremos a simulação de dados para representar os sinais que seriam coletados da máquina de solda. Isso permite:
Criação de um gerador de dados em Python:

Script Python que gera dados de sensores com base em faixas realistas de operação de uma máquina de solda.

**As variáveis simuladas incluem:**
- Temperatura (°C) com variação de 25 °C a 90 °C
- Corrente elétrica (A): entre 100 A e 300 A
- Vibração (m/s²)
- Tempo de operação (s): contador que se acumula a cada ciclo
- Status de falha: binário com os valores 0 (normal) ou 1 (anomalia), com base em regras simples ou probabilidade.

**Simulação de falhas intencionais:**
- Para treinar os modelos de machine learning, o script deve gerar situações de anomalia em determinados momentos.
- Exemplo: após 100 ciclos, temperatura ultrapassa 85 °C com alta vibração, simulando superaquecimento.
___
## 📋 Plano Inicial de Desenvolvimento
1. Levantamento de variáveis críticas de falha.
2. Simulação de coleta de dados com script e I.A(Chat GPT, Gemini, Claude).
3. Desenho do pipeline e arquitetura.
4. Montagem do repositório com README explicativo.
5. Divisão de responsabilidades entre os membros do grupo.

___
## 👤 Divisão de atividades

| Colaborador         | Atividades                               |
| :------------------ | :--------------------------------------- |
|**Lucas Martinelli** | Coleta de dados dos sensores             |
|**Lucas Martinelli** | Transmissão dos dados.                   |
|**Lais Kurahashi**   | Armazenamento dos dados coletados        |
|**Lais Kurahashi**   | processamento inicial dos dados coletados|
|**Davi Ferreira**    | Desenvolvimento de pipelines             |
|**Davi Ferreira**    | Desenvolvimento da vizualização gráfica  |
|**Davi Ferreira**    | Modelagem machine learning               |
|**Davi Ferreira**    | Alertas (Email/Telegram)                 |

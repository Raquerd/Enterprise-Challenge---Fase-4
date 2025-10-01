## GRUPO 23
### Integrantes:
- Lais Kurahashi
- Lucas Martinelli
- Davi DaviFerreira

## Estrutura de pastas
Dentre os arquivos e pastas presentes na raiz do projeto, definem-se:

- assets: aqui estão os arquivos relacionados a elementos não-estruturados deste repositório, como imagens.

- config: Posicione aqui arquivos de configuração que são usados para definir parâmetros e ajustes do projeto.

- scripts: Posicione aqui scripts auxiliares para tarefas específicas do seu projeto. Exemplo: deploy, migrações de banco de dados, backups.

- document: aqui estão todos os documentos do projeto que as atividades poderão pedir. Na subpasta "other", adicione documentos complementares e menos importantes.

- README.md: arquivo que serve como guia e explicação geral sobre o projeto (o mesmo que você está lendo agora).

## Links:
Link Video apresentação projeto: https://youtu.be/IT3Gl05fPYk

## 👤 Divisão de atividades na fase 3 do projeto

| Colaborador         | Atividades                                               |
| :------------------ | :---------------------------------------                 |
|**Lucas Martinelli** | desenvolvimento de modelo de matriz de confusão          |
|**Lais Kurahashi**   | Desenvolvimento do modelo DER                            |
|**Lais Kurahashi**   | Desenvolvimento de código de criação do banco de dados   |
|**Davi Ferreira**    | Geração de dados para machine learning                   |
|**Davi Ferreira**    | Documentação                                             |

# 💡Prevenção de Falhas em Máquina de Solda

## 🔧 Tecnologias Envolvidas
### Monitor de Ativos com ESP32
O sistema é projetado para coletar e analisar dados de vibração, temperatura e corrente elétrica, além de contar ciclos de operação para manutenção preditiva.

**Visão Geral do Projeto**

O objetivo principal é monitorar as condições de operação de um equipamento em tempo real. O sistema utiliza um acelerômetro MPU6050 para medir a vibração, um sensor de temperatura NTC e um sensor de corrente (simulado por um potenciômetro no diagrama) para avaliar o estado da máquina. Os dados são processados pelo ESP32 e enviados para o Monitor Serial, que exibe os valores e emite alertas caso algum parâmetro exceda os limites pré-definidos.

Componentes de Hardware e Diagrama
O circuito é composto por um ESP32 DevKit V4 e três componentes principais de sensoriamento, conforme o diagrama.
|Componente |                        Descrição                      | Conexão no ESP32|
|:----------|:------------------------------------------------------|:----------------|
|ESP32 DevKit V4 | Placa de desenvolvimento principal que executa o código. |	- |
|MPU6050 | Módulo de Unidade de Medição Inercial (IMU) que combina um acelerômetro e um giroscópio. Usado para medir a vibração nos eixos X, Y e Z. | SDA: Pino 21&lt;br>SCL: Pino 22&lt;br>VCC: 3.3V&lt;br>GND: GND |
|Sensor de Temperatura NTC | Um termistor cujo valor de resistência varia com a temperatura. É usado para monitorar a temperatura do equipamento. | OUT: Pino 34 (Analógico)&lt;br>VCC: 3.3V&lt;br>GND: GND |
|Potenciômetro | Usado para simular um sensor de corrente (como o ACS712). Permite variar a tensão na entrada analógica para testar a lógica de leitura de corrente.|SIG: Pino 35 (Analógico)&lt;br>VCC: 3.3V&lt;br>GND: GND |

## Script python
### Nome script: script_principal.ipynb
Este projeto utiliza dados de sensores de equipamentos industriais para prever o risco de falhas. O objetivo é desenvolver um modelo de machine learning capaz de classificar a condição de operação de uma máquina como 'Normal', 'Atenção' ou 'Risco Iminente', com base em leituras de temperatura, vibração e corrente.

#### Como Executar o Script
Clone este repositório:

```Bash
git clone <URL-do-repositório>
Navegue até o diretório do projeto:
```
```Bash
cd <nome-do-diretório>
Certifique-se de que o arquivo Dados_simulados.csv esteja no caminho especificado no script ou ajuste o caminho no arquivo script_principal.ipynb.
```

Execute o notebook Jupyter script_principal.ipynb.

### Metodologia
#### Pré-processamento de Dados
- Os dados são carregados a partir de um arquivo CSV.

- As colunas vl_temperatura e vl_corrente são arredondadas para duas casas decimais.

- A coluna vl_vibracao é convertida para o tipo inteiro.

#### Engenharia de Features
Uma nova coluna, risco, é criada para ser a nossa variável alvo. A classificação de risco é baseada nos valores de temperatura e corrente, de acordo com as seguintes regras:

**Classificação de Risco por Temperatura:**

- Normal: Se a temperatura for menor que 85.

- Atenção: Se a temperatura estiver entre 85 (inclusive) e 95.

- Risco Iminente: Se a temperatura for 95 ou superior.

**Classificação de Risco por Corrente:**

- Normal: Se a corrente for menor ou igual a 152.

- Atenção: Se a corrente for menor que 155.

- Risco Iminente: Se a corrente for 155 ou superior.

Para o modelo preditivo, a classificação de risco baseada na temperatura foi a escolhida como variável alvo.

#### Modelagem
- Divisão dos Dados: O conjunto de dados foi dividido em 70% para treino e 30% para teste, utilizando a estratificação para manter a proporção das classes de risco em ambos os conjuntos.

- Algoritmo: Foi utilizado o RandomForestClassifier da biblioteca scikit-learn.

- Treinamento: O modelo foi treinado com os dados de treino (x_train, y_train).

- Previsão: O modelo treinado foi utilizado para fazer previsões nos dados de teste (x_test).

#### Resultados
O modelo alcançou uma acurácia de 100.00% no conjunto de teste. O relatório de classificação detalhado e a matriz de confusão demonstram o excelente desempenho do modelo na classificação das três categorias de risco.

## Script SQL
### Nome script: codigo sql para criação das tabelas.sql
O objetivo deste banco de dados é armazenar e organizar dados provenientes de sensores acoplados a máquinas industriais em diferentes fábricas. Ele registra leituras de sensores (temperatura, corrente, vibração), informações sobre as máquinas, lotes de produção e alertas de falhas, permitindo análises e monitoramento de desempenho.

### Estrutura do Banco de Dados
O esquema é composto pelas seguintes tabelas:

#### Tabelas de Dimensão (DM)
As tabelas de dimensão armazenam os dados mestres e descritivos das entidades do negócio.

- dm_fabricas: Armazena as informações das fábricas.

- dm_maquina: Contém os dados de cada máquina e sua relação com a fábrica.

- dm_sistemas: Descreve os sistemas de monitoramento associados a cada máquina.

- dm_sensores: Catálogo com as especificações técnicas de cada tipo de sensor.

- dm_lotes: Registra os lotes de produção.

#### Tabelas Fato (FT/FB)
As tabelas fato armazenam as medições e eventos que ocorrem ao longo do tempo.

- ft_reg_sensores: Tabela central que armazena as leituras dos sensores ao longo do tempo.

- fb_alertas: Registra os alertas e falhas ocorridos em cada máquina.

#### diagrama Diagrama Entidade-Relacionamento (DER)

```bash
erDiagram
    dm_fabricas ||--o{ dm_maquina : "possui"
    dm_maquina ||--o{ dm_sistemas : "possui"
    dm_maquina ||--o{ fb_alertas : "gera"
    dm_maquina ||--o{ ft_reg_sensores : "gera leituras de"
    dm_sistemas ||--o{ dm_sensores : "utiliza"
    dm_sistemas ||--o{ ft_reg_sensores : "registra em"
    dm_lotes ||--o{ ft_reg_sensores : "relacionado a"

    dm_fabricas {
        NUMBER id_fabrica PK
        VARCHAR2 nome_fabrica
        VARCHAR2 cidade
        VARCHAR2 estado
    }
    dm_maquina {
        NUMBER id_maquina PK
        VARCHAR2 nome_maquina
        VARCHAR2 fabricante
        VARCHAR2 modelo
        TIMESTAMP dt_instalacao
        NUMBER id_fabrica FK
    }
    dm_sistemas {
        NUMBER id_sistema PK
        NUMBER id_maquina FK
        VARCHAR2 endereco_sistema
        VARCHAR2 nome_sistema
    }
    dm_sensores {
        NUMBER id_sensor PK
        NUMBER id_sistema FK
        VARCHAR2 nome_sensor
        VARCHAR2 tipo_sensor
        VARCHAR2 fabricante_sensor
        FLOAT faixa_operacional_min
        FLOAT faixa_operacional_max
    }
    dm_lotes {
        NUMBER id_lote PK
        TIMESTAMP dt_lote_registro
    }
    fb_alertas {
        NUMBER id_alerta PK
        NUMBER id_maquina FK
        TIMESTAMP timestamp_alerta
        VARCHAR2 tipo_alerta
        VARCHAR2 nivel
        VARCHAR2 status
    }
    ft_reg_sensores {
        NUMBER id_registro PK
        NUMBER id_sistema FK
        NUMBER id_maquina FK
        NUMBER id_lote FK
        TIMESTAMP timestamp_registro
        FLOAT vl_temperatura
        FLOAT vl_corrente
        FLOAT vl_vibracao
    }
```
___

## 📦 Estratégia de Dados
**Coleta simulada:** 

Nesta fase utilizamos a simulação de dados para representar os sinais que seriam coletados da máquina de solda, entretanto os dados, por se tratarem de simulação, não foram utilizados a nivel da produto central do projeto (maquina de solda).
Os dados foram simulados através de uma script feita pelo membro **Davi Ferreira** para evitar os diversos vieses trazidos por dados simulados por I.A.
São simulados dados de todas as tabelas através da script **gerar_dados.ipynb**, onde salva os dados na pasta **config** presente no projeto

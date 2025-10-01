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

Estrutura do Script
O script é dividido em várias seções principais:

Importação de Bibliotecas e Carregamento de Dados:

Bibliotecas: Utiliza bibliotecas essenciais para análise de dados e aprendizado de máquina, incluindo:
- pandas para manipulação de dados.
- scikit-learn para a implementação dos algoritmos de machine learning.
- seaborn e matplotlib para a visualização gráfica dos dados.

Carregamento e Preparação dos Dados: O script lê o arquivo CSV e realiza um pré-processamento inicial, como a conversão do timestamp_ms para segundos e o cálculo da aceleração total (acc_total) a partir dos dados dos eixos X, Y e Z do acelerômetro.
Divisão dos Dados:

Os dados são segmentados em dois conjuntos. Um conjunto (df_ml) é utilizado para treinar os modelos de detecção de anomalias e clusterização, contendo os ciclos iniciais da operação do equipamento. O outro conjunto é reservado para futuras validações e testes.

### Detecção de Anomalias com Isolation Forest:

IsolationFlorest
- **Implementação:** O modelo é aplicado de forma independente para as colunas de temperatura (tempC), corrente (currentA) e aceleração total (acc_total). O script cria novas colunas no DataFrame para indicar se um ponto é uma anomalia (-1) ou um dado normal (1).
- **Visualização:** Gráficos de dispersão são gerados para os primeiros 120 ciclos, exibindo os dados normais em azul e as anomalias detectadas em vermelho. Isso permite uma rápida identificação visual de comportamentos atípicos.

### Clusterização com K-Means:

K-Means
**Implementação:** O script aplica o K-Means aos dados de acc_total, dividindo-os em 3 clusters. Cada cluster representa um nível de vibração ou um estado operacional específico da máquina (por exemplo, "baixo", "médio" e "alto").
Visualização: Um gráfico de dispersão é criado para os primeiros 120 ciclos, colorindo cada ponto de acordo com o cluster ao qual pertence. Isso ajuda a validar os estados de aceleração e a correlacioná-los com as anomalias encontradas.

### Aplicações Práticas
A análise realizada por este script pode gerar diversos insights valiosos para a gestão de ativos e manutenção:

- Manutenção Preditiva: Ao identificar anomalias recorrentes na temperatura, corrente ou vibração, é possível prever falhas antes que elas ocorram. Por exemplo, um aumento gradual na temperatura e na corrente pode indicar um desgaste de rolamentos ou um motor sobrecarregado.
- Otimização de Processos: A análise dos ciclos operacionais pode revelar ineficiências. Se anomalias de vibração ocorrem consistentemente durante uma fase específica do ciclo, pode ser um indicativo da necessidade de ajustar os parâmetros de operação da máquina.
- Análise de Causa Raiz: Ao cruzar as informações de anomalias com os clusters de aceleração, é possível aprofundar a investigação. Por exemplo, pode-se descobrir que a maioria das anomalias de corrente ocorre quando a máquina está em um cluster de alta vibração, sugerindo uma correlação direta entre esses dois fatores.

### Observação
Os teste com os modelos de Machine Learning foram feitos com 120 ciclos devido a quantidade de dados presentes na tabela. Entretanto os testes foram bastante satisfatórios, pois conseguimos identificar as anomalias e clusterizar os dados corretamente.
___

## 📦 Estratégia de Dados
**Coleta simulada:** 

Nesta fase utilizamos a simulação de dados para representar os sinais que seriam coletados da máquina de solda, entretanto os dados, por se tratarem de simulação, não foram utilizados a nivel da produto central do projeto (maquina de solda).
Os dados foram gerados por I.A utilizando códigos de machine learning.

## 👤 Divisão de atividades na fase 2 do projeto

| Colaborador         | Atividades                                               |
| :------------------ | :---------------------------------------                 |
|**Lucas Martinelli** | desenvolvimento de modelo de regressão linear            |
|**Lais Kurahashi**   | Desenvolvimento do ESP32 (diagrama, sketch.ino)          |
|**Davi Ferreira**    | Tratativa de dados                                       |
|**Davi Ferreira**    | Desenvolvimento do modelo de identificação de anomalia   |
|**Davi Ferreira**    | Modelagem de clusterização da aceleração                 |
|**Davi Ferreira**    | Documentação                                             |

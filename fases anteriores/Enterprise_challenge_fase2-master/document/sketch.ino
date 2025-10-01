#include <Wire.h> // biblioteca para comunicação I2C (sensor MPU6050)
#include <MPU6050.h> // biblioteca do sensor MPU6050
MPU6050 mpu; // cria um objeto para o sensor MPU6050

// Pinos analógicos e digitais usados
const int tempPin = 34; // pino do sensor de temperatura (ent analógica)
const int currentPin = 35; // pino do sensor de corrente (ent analógica)
int cycleCount = 0; // contador de ciclos (quantas vezes o botão foi pressionado) 

// Variáveis para controle do tempo do contador automático
unsigned long lastCycleTime = 0; // último tempo que contou um ciclo
const unsigned long cycleInterval = 5000; // intervalo de 10 segundos para contar um ciclo

// Limites de alerta (exemplos)
const float tempLimit = 30.0;     // °C
const float currentLimit = 1.0;   // Amperes
const float vibrationLimit = 0.5; // m/s²

void setup() {
  Serial.begin(115200); // inicializa serial (evita atraso na leitura dos dados)
  Wire.begin();
  mpu.initialize();
  // Inicializa o sensor MPU6050
  if (!mpu.testConnection()) {
    Serial.println("MPU failed"); // se falhar, mostra mensagem
    while (1);
  }
  
  delay(100); 
  Serial.println("Setup completo!"); // confirma que foi inicializado  
}

void loop() {
  // Obtenção dos dados de acelerometro
  int16_t ax, ay, az;
  mpu.getAcceleration(&ax, &ay, &az);

  // Nesse bloco, será feita a divisão dos dados brutos do acelerometro pela escala +2g, que é o valor de 16384 e logo após, multiplicar por 9.81 para obter os valores em m/s²
  float accX_g = (ax / 16384.0) * 9.81;
  float accY_g = (ay / 16384.0) * 9.81;
  float accZ_g = (az / 16384.0) * 9.81;
  
  // Contador automático de ciclos a cada 5 segundos
  unsigned long currentMillis = millis();
  if (currentMillis - lastCycleTime >= cycleInterval) {
    cycleCount++;
    lastCycleTime = currentMillis;
  }

  // Leitura da temperatura (conversão do sinal analógico para °C)
  int rawTemp = analogRead(tempPin);
  float voltage = rawTemp * (3.3 / 4095); // converte para tensão
  float tempC = (voltage - 0.5) * 100.0; // fórmula

  // Leitura da corrente elétrica
  int rawCurr = analogRead(currentPin);
  float currVoltage = rawCurr * (3.3 / 4095); // converte para tensão
  float currentA = (currVoltage - 2.5) / 0.066; // converte para corrente

  // Verificação de limites e geração de alertas
  String alertMsg = "";
  if (tempC > tempLimit) {
    alertMsg += "Alerta: Temperatura alta! ";
  }
  if (currentA > currentLimit) {
    alertMsg += "Alerta: Corrente alta! ";
  }

  // Visualização monitor serial (modo legível)
  Serial.print("Ciclos: "); Serial.print(cycleCount);
  Serial.print(" | Temp: "); Serial.print(tempC); Serial.print("°C");
  Serial.print(" | Corrente: "); Serial.print(currentA, 2); Serial.print(" A");
  Serial.print(" | Acc X: "); Serial.print(accX_g);
  Serial.print(" Y: "); Serial.print(accY_g);
  Serial.print(" Z: "); Serial.println(accZ_g);
  
  if (alertMsg.length() > 0) {
    Serial.println(alertMsg);
  }

  delay(1000); 
}

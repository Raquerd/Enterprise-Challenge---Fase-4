
-- Tabela de Dimensão: dm_fabricas
-- Armazena as informações das fábricas. É uma entidade forte e independente.
-- -----------------------------------------------------
CREATE TABLE dm_fabricas (
    id_fabrica      NUMBER(10)      NOT NULL,
    nome_fabrica    VARCHAR2(100)   NOT NULL,
    cidade          VARCHAR2(100),
    estado          VARCHAR2(500), -- Ajustado para um tamanho maior, como na imagem
    CONSTRAINT pk_fabricas PRIMARY KEY (id_fabrica)
);

-- -----------------------------------------------------
-- Tabela de Dimensão: dm_sensores
-- Catalogo dos tipos de sensores utilizados.
-- -----------------------------------------------------
CREATE TABLE dm_sensores (
    id_sensor           NUMBER(10)              NOT NULL,
    id_sistema          NUMBER(10)              NOT NULL,
    nome_sensor         VARCHAR2(50),
    tipo_sensor         VARCHAR2(100),
    fabricante_sensor   VARCHAR2(100),
    faixa_operacional_min   FLOAT,
    faixa_operacional_max   FLOAT,
    CONSTRAINT pk_sensores PRIMARY KEY (id_sensor),
    CONSTRAINT fk_id_sistemas FOREIGN KEY (id_sistema) REFERENCES dm_sistemas(id_sistema)
);

-- -----------------------------------------------------
-- Tabela de Dimensão: dm_lotes
-- Registra os lotes de produção.
-- -----------------------------------------------------
CREATE TABLE dm_lotes (
    id_lote             NUMBER(20)      NOT NULL,
    dt_lote_registro    TIMESTAMP,
    CONSTRAINT pk_lotes PRIMARY KEY (id_lote)
);

-- -----------------------------------------------------
-- Tabela de Dimensão: dm_maquina
-- Armazena os dados de cada máquina de solda e sua relação com a fábrica.
-- -----------------------------------------------------
CREATE TABLE dm_maquina (
    id_maquina          NUMBER(10)      NOT NULL,
    nome_maquina        VARCHAR2(300)   NOT NULL,
    fabricante          VARCHAR2(200),
    modelo              VARCHAR2(500),
    dt_instalacao       TIMESTAMP,
    id_fabrica          NUMBER(10)      NOT NULL,
    CONSTRAINT pk_maquina PRIMARY KEY (id_maquina),
    CONSTRAINT fk_maquina_fabricas FOREIGN KEY (id_fabrica) REFERENCES dm_fabricas(id_fabrica)
);

-- -----------------------------------------------------
-- Tabela Fato: fb_alertas
-- Registra os alertas e falhas ocorridos em cada máquina.
-- -----------------------------------------------------
CREATE TABLE fb_alertas (
    id_alerta           NUMBER(20)      NOT NULL,
    id_maquina          NUMBER(10)      NOT NULL,
    timestamp_alerta    TIMESTAMP,
    tipo_alerta         VARCHAR2(100), -- O tipo de dado estava como NUMERIC na imagem, ajustado para VARCHAR2 para descrever o alerta
    nivel               VARCHAR2(50),  -- O tipo de dado estava como NUMERIC na imagem, ajustado para VARCHAR2 para descrever o nível
    status              VARCHAR2(50),  -- O tipo de dado estava como NUMERIC na imagem, ajustado para VARCHAR2 para descrever o status
    CONSTRAINT pk_alertas PRIMARY KEY (id_alerta),
    CONSTRAINT fk_alertas_maquina FOREIGN KEY (id_maquina) REFERENCES dm_maquina(id_maquina)
);

-- -----------------------------------------------------
-- Tabela Fato: ft_reg_sensores
-- Tabela central que armazena as leituras dos sensores.
-- -----------------------------------------------------
CREATE TABLE ft_reg_sensores (
    id_registro         NUMBER(20)              NOT NULL,
    id_sistema          NUMBER(10)              NOT NULL,
    id_maquina          NUMBER(10)              NOT NULL,
    id_lote             NUMBER(20),
    timestamp_registro  TIMESTAMP WITH TIME ZONE,
    vl_temperatura      FLOAT,
    vl_corrente         FLOAT,
    vl_vibracao         FLOAT,
    CONSTRAINT pk_reg_sensores PRIMARY KEY (id_registro),
    CONSTRAINT fk_reg_sensores_sistema FOREIGN KEY (id_sistema) REFERENCES dm_sistemas(id_sistema),
    CONSTRAINT fk_reg_sensores_maquina FOREIGN KEY (id_maquina) REFERENCES dm_maquina(id_maquina),
    CONSTRAINT fk_reg_sensores_lote FOREIGN KEY (id_lote) REFERENCES dm_lotes(id_lote)
);

CREATE TABLE dm_sistemas (
    id_sistema              NUMBER(20)              NOT NULL,
    id_maquina              NUMBER(10)              NOT NULL,
    endereco_sistema        VARCHAR2(200),
    nome_sistema            VARCHAR2(100),
    CONSTRAINT pk_dm_sistemas PRIMARY KEY (id_sistema),
    CONSTRAINT fk_dm_sistemas FOREIGN KEY (id_maquina) REFERENCES dm_maquina(id_maquina)
);
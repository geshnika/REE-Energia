-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--                                                    Tabla: Generacion                                                      --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

CREATE TABLE ree.Generacion (
    Id          INT IDENTITY(1,1) PRIMARY KEY,
    Fecha       DATE,
    Fuente      NVARCHAR(100),
    Valor_mwh   FLOAT,
    Porcentaje  FLOAT,
    FechaAlta   DATE DEFAULT CAST(GETDATE() AS DATE),
    HoraAlta    TIME DEFAULT CAST(GETDATE() AS TIME)
);

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--                                                      Tabla: Demanda                                                       --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

CREATE TABLE ree.Demanda (
    Id          INT IDENTITY(1,1) PRIMARY KEY,
    Fecha       DATE,
    Tipo        NVARCHAR(100),
    Valor_mwh   FLOAT,
    FechaAlta   DATE DEFAULT CAST(GETDATE() AS DATE),
    HoraAlta    TIME DEFAULT CAST(GETDATE() AS TIME)
);

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--                                                     Tabla: Emisiones                                                      --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

CREATE TABLE ree.Emisiones (
    Id          INT IDENTITY(1,1) PRIMARY KEY,
    Fecha       DATE,
    Tipo        NVARCHAR(100),
    Valor       FLOAT,
    FechaAlta   DATE DEFAULT CAST(GETDATE() AS DATE),
    HoraAlta    TIME DEFAULT CAST(GETDATE() AS TIME)
);

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--                                                      Tabla: Precios                                                       --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

CREATE TABLE ree.Precios (
    Id            INT IDENTITY(1,1) PRIMARY KEY,
    Fecha         DATE,
    Hora          TIME,
    Zona          NVARCHAR(100),
    Tipo          NVARCHAR(100),
    Valor_eur_mwh FLOAT,
    FechaAlta     DATE DEFAULT CAST(GETDATE() AS DATE),
    HoraAlta      TIME DEFAULT CAST(GETDATE() AS TIME)
);

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--                                                    Tabla: Intercambios                                                    --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

CREATE TABLE ree.Intercambios (
    Id          INT IDENTITY(1,1) PRIMARY KEY,
    Fecha       DATE,
    Pais        NVARCHAR(100),
    Tipo        NVARCHAR(100),
    Valor_mwh   FLOAT,
    FechaAlta   DATE DEFAULT CAST(GETDATE() AS DATE),
    HoraAlta    TIME DEFAULT CAST(GETDATE() AS TIME)
);

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--                                                     Tabla: Historico                                                      --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

CREATE TABLE ree.Historico (
    IdHistorico INT IDENTITY(1,1) PRIMARY KEY,
    Tabla       NVARCHAR(100),
    Id          INT,
    Columna     NVARCHAR(100),
    ValorPrevio NVARCHAR(MAX),
    ValorNuevo  NVARCHAR(MAX),
    FechaAlta   DATE DEFAULT CAST(GETDATE() AS DATE),
    HoraAlta    TIME DEFAULT CAST(GETDATE() AS TIME)
);

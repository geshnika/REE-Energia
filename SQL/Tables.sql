-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--                                                    Tabla: Generacion                                                      --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

CREATE TABLE Generacion (
    Id          INT IDENTITY(1,1) PRIMARY KEY,
    Fecha       DATE,
    Fuente      NVARCHAR(100),
    Valor_mwh   FLOAT,
    Porcentaje  FLOAT,
    FechaAlta   DATE DEFAULT CAST(GETDATE() AS DATE),
    HoraAlta    TIME DEFAULT CAST(GETDATE() AS TIME)
)
;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--                                                      Tabla: Demanda                                                        --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

CREATE TABLE Demanda (
    Id          INT IDENTITY(1,1) PRIMARY KEY,
    Fecha       DATE,
    Tipo        NVARCHAR(100),
    Valor_mwh   FLOAT,
    FechaAlta   DATE DEFAULT CAST(GETDATE() AS DATE),
    HoraAlta    TIME DEFAULT CAST(GETDATE() AS TIME)
)
;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--                                                     Tabla: Emisiones                                                       --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

CREATE TABLE Emisiones (
    Id          INT IDENTITY(1,1) PRIMARY KEY,
    Fecha       DATE,
    Tipo        NVARCHAR(100),
    Valor       FLOAT,
    FechaAlta   DATE DEFAULT CAST(GETDATE() AS DATE),
    HoraAlta    TIME DEFAULT CAST(GETDATE() AS TIME)
)
;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--                                                      Tabla: Precios                                                        --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

CREATE TABLE Precios (
    Id            INT IDENTITY(1,1) PRIMARY KEY,
    Fecha         DATE,
    Tipo          NVARCHAR(100),
    Valor_eur_mwh FLOAT,
    FechaAlta     DATE DEFAULT CAST(GETDATE() AS DATE),
    HoraAlta      TIME DEFAULT CAST(GETDATE() AS TIME)
)
;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--                                                    Tabla: Intercambios                                                     --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

CREATE TABLE Intercambios (
    Id          INT IDENTITY(1,1) PRIMARY KEY,
    Fecha       DATE,
    Pais        NVARCHAR(100),
    Tipo        NVARCHAR(100),
    Valor_mwh   FLOAT,
    FechaAlta   DATE DEFAULT CAST(GETDATE() AS DATE),
    HoraAlta    TIME DEFAULT CAST(GETDATE() AS TIME)
)
;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--                                                     Tabla: Historico                                                       --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

CREATE TABLE Historico (
    IdHistorico INT IDENTITY(1,1) PRIMARY KEY,
    Tabla       NVARCHAR(100),
    Id          INT,
    Columna     NVARCHAR(100),
    ValorPrevio NVARCHAR(MAX),
    ValorNuevo  NVARCHAR(MAX),
    FechaAlta   DATE DEFAULT CAST(GETDATE() AS DATE),
    HoraAlta    TIME DEFAULT CAST(GETDATE() AS TIME)
)
;
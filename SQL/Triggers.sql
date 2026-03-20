-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--                                                 Triggers: Tabla Generacion                                                 --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

CREATE TRIGGER TRG_GeneracionHistorico
ON Generacion
AFTER UPDATE
AS

BEGIN

    INSERT INTO Historico (Tabla, Id, Columna, ValorPrevio, ValorNuevo)
    SELECT 
          'Generacion'
          ,I.Id
          ,'Fecha'
          ,CAST(D.Fecha AS NVARCHAR(MAX))
          ,CAST(I.Fecha AS NVARCHAR(MAX))
    FROM Inserted AS I
    INNER JOIN Deleted AS D ON I.Id = D.Id
    WHERE ISNULL(CAST(I.Fecha AS NVARCHAR(MAX)), '') <> ISNULL(CAST(D.Fecha AS NVARCHAR(MAX)), '');

    INSERT INTO Historico (Tabla, Id, Columna, ValorPrevio, ValorNuevo)
    SELECT 
          'Generacion'
          ,I.Id
          ,'Fuente'
          ,D.Fuente
          ,I.Fuente
    FROM Inserted AS I
    INNER JOIN Deleted AS D ON I.Id = D.Id
    WHERE ISNULL(I.Fuente, '') <> ISNULL(D.Fuente, '');

    INSERT INTO Historico (Tabla, Id, Columna, ValorPrevio, ValorNuevo)
    SELECT 
          'Generacion'
          ,I.Id
          ,'Valor_mwh'
          ,CAST(D.Valor_mwh AS NVARCHAR(MAX))
          ,CAST(I.Valor_mwh AS NVARCHAR(MAX))
    FROM Inserted AS I
    INNER JOIN Deleted AS D ON I.Id = D.Id
    WHERE ISNULL(I.Valor_mwh, -1) <> ISNULL(D.Valor_mwh, -1);

    INSERT INTO Historico (Tabla, Id, Columna, ValorPrevio, ValorNuevo)
    SELECT 
          'Generacion'
          ,I.Id
          ,'Porcentaje'
          ,CAST(D.Porcentaje AS NVARCHAR(MAX))
          ,CAST(I.Porcentaje AS NVARCHAR(MAX))
    FROM Inserted AS I
    INNER JOIN Deleted AS D ON I.Id = D.Id
    WHERE ISNULL(I.Porcentaje, -1) <> ISNULL(D.Porcentaje, -1);

END;

GO

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--                                                  Triggers: Tabla Demanda                                                   --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

CREATE TRIGGER TRG_DemandaHistorico
ON Demanda
AFTER UPDATE
AS

BEGIN

    INSERT INTO Historico (Tabla, Id, Columna, ValorPrevio, ValorNuevo)
    SELECT
          'Demanda'
          ,I.Id
          ,'Fecha'
          ,CAST(D.Fecha AS NVARCHAR(MAX))
          ,CAST(I.Fecha AS NVARCHAR(MAX))
    FROM Inserted AS I
    INNER JOIN Deleted AS D ON I.Id = D.Id
    WHERE ISNULL(CAST(I.Fecha AS NVARCHAR(MAX)), '') <> ISNULL(CAST(D.Fecha AS NVARCHAR(MAX)), '');

    INSERT INTO Historico (Tabla, Id, Columna, ValorPrevio, ValorNuevo)
    SELECT 
          'Demanda'
          ,I.Id
          ,'Tipo'
          ,D.Tipo
          ,I.Tipo
    FROM Inserted AS I
    INNER JOIN Deleted AS D ON I.Id = D.Id
    WHERE ISNULL(I.Tipo, '') <> ISNULL(D.Tipo, '');

    INSERT INTO Historico (Tabla, Id, Columna, ValorPrevio, ValorNuevo)
    SELECT 
          'Demanda'
          ,I.Id
          ,'Valor_mwh'
          ,CAST(D.Valor_mwh AS NVARCHAR(MAX))
          ,CAST(I.Valor_mwh AS NVARCHAR(MAX))
    FROM Inserted AS I
    INNER JOIN Deleted AS D ON I.Id = D.Id
    WHERE ISNULL(I.Valor_mwh, -1) <> ISNULL(D.Valor_mwh, -1);

END;

GO

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--                                                 Triggers: Tabla Emisiones                                                  --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

CREATE TRIGGER TRG_EmisionesHistorico
ON Emisiones
AFTER UPDATE
AS

BEGIN

    INSERT INTO Historico (Tabla, Id, Columna, ValorPrevio, ValorNuevo)
    SELECT 
          'Emisiones'
          ,I.Id
          ,'Fecha'
          ,CAST(D.Fecha AS NVARCHAR(MAX))
          ,CAST(I.Fecha AS NVARCHAR(MAX))
    FROM Inserted AS I
    INNER JOIN Deleted AS D ON I.Id = D.Id
    WHERE ISNULL(CAST(I.Fecha AS NVARCHAR(MAX)), '') <> ISNULL(CAST(D.Fecha AS NVARCHAR(MAX)), '');

    INSERT INTO Historico (Tabla, Id, Columna, ValorPrevio, ValorNuevo)
    SELECT 
          'Emisiones'
          ,I.Id
          ,'Tipo'
          ,D.Tipo
          ,I.Tipo
    FROM Inserted AS I
    INNER JOIN Deleted AS D ON I.Id = D.Id
    WHERE ISNULL(I.Tipo, '') <> ISNULL(D.Tipo, '');

    INSERT INTO Historico (Tabla, Id, Columna, ValorPrevio, ValorNuevo)
    SELECT 
          'Emisiones'
          ,I.Id
          ,'Valor'
          ,CAST(D.Valor AS NVARCHAR(MAX))
          ,CAST(I.Valor AS NVARCHAR(MAX))
    FROM Inserted AS I
    INNER JOIN Deleted AS D ON I.Id = D.Id
    WHERE ISNULL(I.Valor, -1) <> ISNULL(D.Valor, -1);

END;

GO

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--                                                  Triggers: Tabla Precios                                                   --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

CREATE TRIGGER TRG_PreciosHistorico
ON Precios
AFTER UPDATE
AS

BEGIN

    INSERT INTO Historico (Tabla, Id, Columna, ValorPrevio, ValorNuevo)
    SELECT 
          'Precios'
          ,I.Id
          ,'Fecha'
          ,CAST(D.Fecha AS NVARCHAR(MAX))
          ,CAST(I.Fecha AS NVARCHAR(MAX))
    FROM Inserted AS I
    INNER JOIN Deleted AS D ON I.Id = D.Id
    WHERE ISNULL(CAST(I.Fecha AS NVARCHAR(MAX)), '') <> ISNULL(CAST(D.Fecha AS NVARCHAR(MAX)), '');

    INSERT INTO Historico (Tabla, Id, Columna, ValorPrevio, ValorNuevo)
    SELECT 
          'Precios'
          ,I.Id
          ,'Tipo'
          ,D.Tipo
          ,I.Tipo
    FROM Inserted AS I
    INNER JOIN Deleted AS D ON I.Id = D.Id
    WHERE ISNULL(I.Tipo, '') <> ISNULL(D.Tipo, '');

    INSERT INTO Historico (Tabla, Id, Columna, ValorPrevio, ValorNuevo)
    SELECT 
          'Precios'
          ,I.Id
          ,'Valor_eur_mwh'
          ,CAST(D.Valor_eur_mwh AS NVARCHAR(MAX))
          ,CAST(I.Valor_eur_mwh AS NVARCHAR(MAX))
    FROM Inserted AS I
    INNER JOIN Deleted AS D ON I.Id = D.Id
    WHERE ISNULL(I.Valor_eur_mwh, -1) <> ISNULL(D.Valor_eur_mwh, -1);

END;

GO

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--                                                Triggers: Tabla Intercambios                                                --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

CREATE TRIGGER TRG_IntercambiosHistorico
ON Intercambios
AFTER UPDATE
AS

BEGIN

    INSERT INTO Historico (Tabla, Id, Columna, ValorPrevio, ValorNuevo)
    SELECT 
          'Intercambios'
          ,I.Id
          ,'Fecha'
          ,CAST(D.Fecha AS NVARCHAR(MAX))
          ,CAST(I.Fecha AS NVARCHAR(MAX))
    FROM Inserted AS I
    INNER JOIN Deleted AS D ON I.Id = D.Id
    WHERE ISNULL(CAST(I.Fecha AS NVARCHAR(MAX)), '') <> ISNULL(CAST(D.Fecha AS NVARCHAR(MAX)), '');

    INSERT INTO Historico (Tabla, Id, Columna, ValorPrevio, ValorNuevo)
    SELECT 
          'Intercambios'
          ,I.Id
          ,'Pais'
          ,D.Pais
          ,I.Pais
    FROM Inserted AS I
    INNER JOIN Deleted AS D ON I.Id = D.Id
    WHERE ISNULL(I.Pais, '') <> ISNULL(D.Pais, '');

    INSERT INTO Historico (Tabla, Id, Columna, ValorPrevio, ValorNuevo)
    SELECT 
          'Intercambios'
          ,I.Id
          ,'Tipo'
          ,D.Tipo
          ,I.Tipo
    FROM Inserted AS I
    INNER JOIN Deleted AS D ON I.Id = D.Id
    WHERE ISNULL(I.Tipo, '') <> ISNULL(D.Tipo, '');

    INSERT INTO Historico (Tabla, Id, Columna, ValorPrevio, ValorNuevo)
    SELECT 
          'Intercambios'
          ,I.Id
          ,'Valor_mwh'
          ,CAST(D.Valor_mwh AS NVARCHAR(MAX))
          ,CAST(I.Valor_mwh AS NVARCHAR(MAX))
    FROM Inserted AS I
    INNER JOIN Deleted AS D ON I.Id = D.Id
    WHERE ISNULL(I.Valor_mwh, -1) <> ISNULL(D.Valor_mwh, -1);

END;

GO
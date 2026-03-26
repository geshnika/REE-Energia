-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--														 CTE: Inicio														  --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

WITH
  PreciosDiarios AS ( -- -- -- -- -- -- -- -- - Tabla Precios: Resumen anual/mensual con valores promedio, minimos y maximos <<<
    SELECT
         YEAR(Fecha) AS Año
        ,MONTH(Fecha) AS Mes
        ,CAST(AVG(Valor_eur_mwh) AS DECIMAL(10, 2)) AS AVG_Valor
        ,CAST(MIN(Valor_eur_mwh) AS DECIMAL(10, 2)) AS MIN_Valor
        ,CAST(MAX(Valor_eur_mwh) AS DECIMAL(10, 2)) AS MAX_Valor
    FROM Precios
    GROUP BY
         YEAR(Fecha)
        ,MONTH(Fecha)
)

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--														  CTE: Fin														      --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

SELECT -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- - - Union all con años por fila y meses por columna <<<
     ROW_NUMBER() OVER (ORDER BY Año ASC, TipoPrecio DESC) AS Id
    ,*
FROM (
    SELECT
         Año
        ,'AVG' AS TipoPrecio
        ,CAST(AVG(IIF(Mes = 1,  AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Jan
        ,CAST(AVG(IIF(Mes = 2,  AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Feb
        ,CAST(AVG(IIF(Mes = 3,  AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Mar
        ,CAST(AVG(IIF(Mes = 4,  AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Apr
        ,CAST(AVG(IIF(Mes = 5,  AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS May
        ,CAST(AVG(IIF(Mes = 6,  AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Jun
        ,CAST(AVG(IIF(Mes = 7,  AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Jul
        ,CAST(AVG(IIF(Mes = 8,  AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Aug
        ,CAST(AVG(IIF(Mes = 9,  AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Sep
        ,CAST(AVG(IIF(Mes = 10, AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Oct
        ,CAST(AVG(IIF(Mes = 11, AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Nov
        ,CAST(AVG(IIF(Mes = 12, AVG_Valor, NULL)) AS DECIMAL(5, 2)) AS Dec
        ,CAST(AVG(AVG_Valor) AS DECIMAL(5, 2)) AS Total
    FROM PreciosDiarios
    GROUP BY Año

    UNION ALL

    SELECT
         Año
        ,'MAX' AS TipoPrecio
        ,MAX(IIF(Mes = 1,  MAX_Valor, NULL)) AS Jan
        ,MAX(IIF(Mes = 2,  MAX_Valor, NULL)) AS Feb
        ,MAX(IIF(Mes = 3,  MAX_Valor, NULL)) AS Mar
        ,MAX(IIF(Mes = 4,  MAX_Valor, NULL)) AS Apr
        ,MAX(IIF(Mes = 5,  MAX_Valor, NULL)) AS May
        ,MAX(IIF(Mes = 6,  MAX_Valor, NULL)) AS Jun
        ,MAX(IIF(Mes = 7,  MAX_Valor, NULL)) AS Jul
        ,MAX(IIF(Mes = 8,  MAX_Valor, NULL)) AS Aug
        ,MAX(IIF(Mes = 9,  MAX_Valor, NULL)) AS Sep
        ,MAX(IIF(Mes = 10, MAX_Valor, NULL)) AS Oct
        ,MAX(IIF(Mes = 11, MAX_Valor, NULL)) AS Nov
        ,MAX(IIF(Mes = 12, MAX_Valor, NULL)) AS Dec
        ,CAST(AVG(MAX_Valor) AS DECIMAL(5, 2)) AS Total
    FROM PreciosDiarios
    GROUP BY Año

    UNION ALL

    SELECT
         Año
        ,'MIN' AS TipoPrecio
        ,MIN(IIF(Mes = 1,  MIN_Valor, NULL)) AS Jan
        ,MIN(IIF(Mes = 2,  MIN_Valor, NULL)) AS Feb
        ,MIN(IIF(Mes = 3,  MIN_Valor, NULL)) AS Mar
        ,MIN(IIF(Mes = 4,  MIN_Valor, NULL)) AS Apr
        ,MIN(IIF(Mes = 5,  MIN_Valor, NULL)) AS May
        ,MIN(IIF(Mes = 6,  MIN_Valor, NULL)) AS Jun
        ,MIN(IIF(Mes = 7,  MIN_Valor, NULL)) AS Jul
        ,MIN(IIF(Mes = 8,  MIN_Valor, NULL)) AS Aug
        ,MIN(IIF(Mes = 9,  MIN_Valor, NULL)) AS Sep
        ,MIN(IIF(Mes = 10, MIN_Valor, NULL)) AS Oct
        ,MIN(IIF(Mes = 11, MIN_Valor, NULL)) AS Nov
        ,MIN(IIF(Mes = 12, MIN_Valor, NULL)) AS Dec
        ,CAST(MIN(MIN_Valor) AS DECIMAL(5, 2)) AS Total
    FROM PreciosDiarios
    GROUP BY Año
    ) AS Ev

ORDER BY Id DESC
;
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--														 CTE: Inicio														  --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

WITH
  PreciosDiarios AS ( -- -- -- -- -- -- -- -- - Tabla Precios: Resumen anual/mensual con valores promedio, minimos y maximos <<<
    SELECT
         YEAR(Fecha) AS Año
        ,MONTH(Fecha) AS Mes
        ,CAST(AVG(Valor_EUR_MWH) AS DECIMAL(10, 2)) AS AVG_Valor
        ,CAST(MIN(Valor_EUR_MWH) AS DECIMAL(10, 2)) AS MIN_Valor
        ,CAST(MAX(Valor_EUR_MWH) AS DECIMAL(10, 2)) AS MAX_Valor
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
        ,CAST(AVG(IIF(Mes = 1, AVG_Valor, NULL)) AS DECIMAL (5, 2)) AS Enero
        ,CAST(AVG(IIF(Mes = 2, AVG_Valor, NULL)) AS DECIMAL (5, 2)) AS Febrero
        ,CAST(AVG(IIF(Mes = 3, AVG_Valor, NULL)) AS DECIMAL (5, 2)) AS Marzo
        ,CAST(AVG(IIF(Mes = 4, AVG_Valor, NULL)) AS DECIMAL (5, 2)) AS Abril
        ,CAST(AVG(IIF(Mes = 5, AVG_Valor, NULL)) AS DECIMAL (5, 2)) AS Mayo
        ,CAST(AVG(IIF(Mes = 6, AVG_Valor, NULL)) AS DECIMAL (5, 2)) AS Junio
        ,CAST(AVG(IIF(Mes = 7, AVG_Valor, NULL)) AS DECIMAL (5, 2)) AS Julio
        ,CAST(AVG(IIF(Mes = 8, AVG_Valor, NULL)) AS DECIMAL (5, 2)) AS Agosto
        ,CAST(AVG(IIF(Mes = 9, AVG_Valor, NULL)) AS DECIMAL (5, 2)) AS Septiembre
        ,CAST(AVG(IIF(Mes = 10, AVG_Valor, NULL)) AS DECIMAL (5, 2)) AS Octubre
        ,CAST(AVG(IIF(Mes = 11, AVG_Valor, NULL)) AS DECIMAL (5, 2)) AS Noviembre
        ,CAST(AVG(IIF(Mes = 12, AVG_Valor, NULL)) AS DECIMAL (5, 2)) AS Diciembre
        ,CAST(AVG(AVG_Valor) AS DECIMAL (5, 2)) AS Total
    FROM PreciosDiarios
    GROUP BY Año
    
    UNION ALL
    
    SELECT
         Año
        ,'MAX' AS Precio
        ,MAX(IIF(Mes = 1, MAX_Valor, NULL)) AS Enero
        ,MAX(IIF(Mes = 2, MAX_Valor, NULL)) AS Febrero
        ,MAX(IIF(Mes = 3, MAX_Valor, NULL)) AS Marzo
        ,MAX(IIF(Mes = 4, MAX_Valor, NULL)) AS Abril
        ,MAX(IIF(Mes = 5, MAX_Valor, NULL)) AS Mayo
        ,MAX(IIF(Mes = 6, MAX_Valor, NULL)) AS Junio
        ,MAX(IIF(Mes = 7, MAX_Valor, NULL)) AS Julio
        ,MAX(IIF(Mes = 8, MAX_Valor, NULL)) AS Agosto
        ,MAX(IIF(Mes = 9, MAX_Valor, NULL)) AS Septiembre
        ,MAX(IIF(Mes = 10, MAX_Valor, NULL)) AS Octubre
        ,MAX(IIF(Mes = 11, MAX_Valor, NULL)) AS Noviembre
        ,MAX(IIF(Mes = 12, MAX_Valor, NULL)) AS Diciembre
        ,CAST(AVG(MAX_Valor) AS DECIMAL (5, 2)) AS Total
    FROM PreciosDiarios
    GROUP BY Año
    
    UNION ALL
    
    SELECT
         Año
        ,'MIN' AS Precio
        ,MIN(IIF(Mes = 1, MIN_Valor, NULL)) AS Enero
        ,MIN(IIF(Mes = 2, MIN_Valor, NULL)) AS Febrero
        ,MIN(IIF(Mes = 3, MIN_Valor, NULL)) AS Marzo
        ,MIN(IIF(Mes = 4, MIN_Valor, NULL)) AS Abril
        ,MIN(IIF(Mes = 5, MIN_Valor, NULL)) AS Mayo
        ,MIN(IIF(Mes = 6, MIN_Valor, NULL)) AS Junio
        ,MIN(IIF(Mes = 7, MIN_Valor, NULL)) AS Julio
        ,MIN(IIF(Mes = 8, MIN_Valor, NULL)) AS Agosto
        ,MIN(IIF(Mes = 9, MIN_Valor, NULL)) AS Septiembre
        ,MIN(IIF(Mes = 10, MIN_Valor, NULL)) AS Octubre
        ,MIN(IIF(Mes = 11, MIN_Valor, NULL)) AS Noviembre
        ,MIN(IIF(Mes = 12, MIN_Valor, NULL)) AS Diciembre
        ,CAST(MIN(MIN_Valor) AS DECIMAL (5, 2)) AS Total
    FROM PreciosDiarios
    GROUP BY Año
    ) AS Ev

ORDER BY Id DESC
;
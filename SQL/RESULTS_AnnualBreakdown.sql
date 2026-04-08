WITH 
  GeneracionBase AS ( -- -- -- -- -- -- -- -- -- -- -- -- -- -- CTE GeneracionBase: Identificar fuentes y generación por año <<<
	SELECT
		 YEAR(Fecha) AS Año

		,ROW_NUMBER() OVER ( -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- TOP de generación a nivel global >>>
			PARTITION BY YEAR(Fecha) 
			ORDER BY SUM(Valor_Mwh) DESC
			) AS RNGeneracion

		,CASE -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- TOP de generación a nivel renovable >>>
			WHEN Renovable = 1
			THEN ROW_NUMBER() OVER (
				PARTITION BY YEAR(Fecha) 
				ORDER BY 
					 IIF(Renovable = 1, 0, 1)
					,SUM(Valor_Mwh) DESC
					)
			ELSE 0
			END AS RNRenovable

		,Renovable
		,Fuente
		,SUM(Valor_Mwh) AS Generacion

	FROM ( -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- - - Tabla Generacion: identificar generación con energias renovables <<<
		SELECT
		     Fecha
			,CASE
				WHEN Fuente IN ( -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- - De sumarse una nueva fuente, agregar aquí >>>
					 'Eólica'
					,'Hidráulica'
					,'Solar térmica'
					,'Solar fotovoltaica'
					,'Otras renovables'
					,'Residuos renovables'
					)
				THEN 1
				ELSE 0
				END AS Renovable
		    ,Fuente
		    ,Valor_mwh
		FROM ree.Generacion
		WHERE Fuente <> 'Generación total' -- Excluimos 'Generación total', ya que viene incluida en la API.
		) AS ETL

	GROUP BY
		 YEAR(Fecha)
		,Fuente
		,Renovable
),

  GeneracionComparada AS ( -- -- -- -- -- -- -- -- - - CTE GeneracionComparada: Resumen comparativo anual a nivel generación <<<
	SELECT
		 ROW_NUMBER() OVER (ORDER BY Año ASC) AS Id
		,Año
		,SUM(IIF(Renovable = 1, Generacion, 0)) AS GeneracionRenovable
		,SUM(IIF(Renovable = 1, Generacion, 0)) / SUM(Generacion) * 100.00 AS PctGeneracionRenovable
		,SUM(Generacion) AS GeneracionTotal
		,MAX(IIF(RNGeneracion = 1, Fuente, NULL)) AS TipoGeneracion_Top1
		,MAX(IIF(RNGeneracion = 1, Generacion, NULL)) AS ValorGeneracion_Top1
		,MAX(IIF(RNRenovable = 1, Fuente, NULL)) AS TipoRenovable_Top1
		,MAX(IIF(RNRenovable = 1, Generacion, NULL)) AS ValorRenovable_Top1
	FROM GeneracionBase
	GROUP BY Año
),

  Consumo AS ( -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- - Tabla Demanda: Resumen de consumo por año <<<
	SELECT
		 YEAR(Fecha) AS Año
		,SUM(Valor_Mwh) AS ConsumoTotal
	FROM ree.Demanda
	GROUP BY YEAR(Fecha)
),

  IntercambiosPorPais AS (
    SELECT
         YEAR(Fecha) AS Año
        ,Pais
        ,ROUND(SUM(IIF(Tipo = 'Exportación', Valor_mwh, 0)), 2) AS Exportacion
        ,ROUND(SUM(IIF(Tipo = 'Importación', Valor_mwh, 0)), 2) AS Importacion
        ,ROUND(SUM(IIF(Tipo = 'saldo', Valor_mwh, 0)), 2) AS Saldo
        ,ROW_NUMBER() OVER (PARTITION BY YEAR(Fecha) ORDER BY SUM(IIF(Tipo = 'Exportación', Valor_mwh, 0)) ASC) AS TopExporter
        ,ROW_NUMBER() OVER (PARTITION BY YEAR(Fecha) ORDER BY SUM(IIF(Tipo = 'Importación', Valor_mwh, 0)) DESC) AS TopImporter
    FROM ree.Intercambios
    GROUP BY
         YEAR(Fecha)
        ,Pais
)

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--														  CTE: Fin														      --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

SELECT
	 Gc.Id
	,Gc.Año AS [Year]
	,ROUND(Gc.GeneracionRenovable, 2) AS RenewableGeneration
	,ROUND(Gc.GeneracionTotal, 2) AS TotalGeneration
	,ROUND(PctGeneracionRenovable, 2) AS PctRenewable
	,ROUND(Cc.ConsumoTotal, 2) AS TotalConsumption
	,ROUND(100.00 * Gc.GeneracionRenovable / Cc.ConsumoTotal, 2) AS PctRenewableConsumption
	,ROUND(100.00 * Cc.ConsumoTotal / Gc.GeneracionTotal, 2) AS PctConsumption
	,TipoGeneracion_Top1 AS TopSource
	,ROUND(ValorGeneracion_Top1, 2) AS TopSourceMWh
	,TipoRenovable_Top1 AS TopRenewable
	,ROUND(ValorRenovable_Top1, 2) AS TopRenewableMWh
    ,Ex.Pais AS TopExporter
    ,Im.Pais AS TopImporter

FROM GeneracionComparada AS Gc
LEFT JOIN Consumo AS Cc ON Cc.Año = Gc.Año
LEFT JOIN IntercambiosPorPais AS Ex ON Ex.Año = Gc.Año AND Ex.TopExporter = 1
LEFT JOIN IntercambiosPorPais AS Im ON Im.Año = Gc.Año AND Im.TopImporter = 1
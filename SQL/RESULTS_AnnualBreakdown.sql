-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--														 CTE: Inicio														  --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

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
		FROM Generacion
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
	FROM Demanda
	GROUP BY YEAR(Fecha)
)

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
--														  CTE: Fin														      --
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

SELECT
	 Gc.Id
	,Gc.Año
	,ROUND(Gc.GeneracionRenovable, 2) AS GeneracionRenovable
	,ROUND(Gc.GeneracionTotal, 2) AS GeneracionTotal
	,ROUND(PctGeneracionRenovable, 2) AS PctGeneracionRenovable
	,ROUND(Cc.ConsumoTotal, 2) AS ConsumoTotal
	,ROUND(100.00 * Gc.GeneracionRenovable / Cc.ConsumoTotal, 2) AS PctConsumoRenovable
	,ROUND(100.00 * Cc.ConsumoTotal / Gc.GeneracionTotal, 2) AS PctGeneracionConsumida
	,TipoGeneracion_Top1
	,ROUND(ValorGeneracion_Top1, 2) AS ValorGeneracion_Top1
	,TipoRenovable_Top1
	,ROUND(ValorRenovable_Top1, 2) AS ValorRenovable_Top1

FROM GeneracionComparada AS Gc
LEFT JOIN Consumo AS Cc ON Cc.Año = Gc.Año
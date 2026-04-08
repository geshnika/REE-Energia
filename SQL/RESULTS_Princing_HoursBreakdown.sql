SELECT
	 ROW_NUMBER() OVER (ORDER BY Año ASC, TipoPrecio DESC) AS Id
	,*

FROM ( -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- - - Tabla Precios: Segmentación horaria en tramos PVPC <<<
	SELECT
		 YEAR(Fecha) AS Año
		,'AVG' AS TipoPrecio
		,CAST(AVG(IIF(Hora BETWEEN '00:00:00' AND '07:59:00', Valor_eur_mwh, NULL)) AS DECIMAL (5, 2)) AS [00:00 - 07:59]
		,CAST(AVG(IIF(Hora BETWEEN '08:00:00' AND '09:59:00', Valor_eur_mwh, NULL)) AS DECIMAL (5, 2)) AS [08:00 - 09:59]
		,CAST(AVG(IIF(Hora BETWEEN '10:00:00' AND '13:59:00', Valor_eur_mwh, NULL)) AS DECIMAL (5, 2)) AS [10:00 - 13:59]
		,CAST(AVG(IIF(Hora BETWEEN '14:00:00' AND '17:59:00', Valor_eur_mwh, NULL)) AS DECIMAL (5, 2)) AS [14:00 - 17:59]
		,CAST(AVG(IIF(Hora BETWEEN '18:00:00' AND '21:59:00', Valor_eur_mwh, NULL)) AS DECIMAL (5, 2)) AS [18:00 - 21:59]
		,CAST(AVG(IIF(Hora BETWEEN '22:00:00' AND '23:59:00', Valor_eur_mwh, NULL)) AS DECIMAL (5, 2)) AS [22:00 - 23:59]
	FROM ree.Precios
	GROUP BY YEAR(Fecha)
	
	UNION ALL
	
	SELECT
		 YEAR(Fecha) AS Año
		,'MAX' AS TipoPrecio
		,MAX(IIF(Hora BETWEEN '00:00:00' AND '07:59:00', Valor_eur_mwh, NULL)) AS [00:00 - 07:59]
		,MAX(IIF(Hora BETWEEN '08:00:00' AND '09:59:00', Valor_eur_mwh, NULL)) AS [08:00 - 09:59]
		,MAX(IIF(Hora BETWEEN '10:00:00' AND '13:59:00', Valor_eur_mwh, NULL)) AS [10:00 - 13:59]
		,MAX(IIF(Hora BETWEEN '14:00:00' AND '17:59:00', Valor_eur_mwh, NULL)) AS [14:00 - 17:59]
		,MAX(IIF(Hora BETWEEN '18:00:00' AND '21:59:00', Valor_eur_mwh, NULL)) AS [18:00 - 21:59]
		,MAX(IIF(Hora BETWEEN '22:00:00' AND '23:59:00', Valor_eur_mwh, NULL)) AS [22:00 - 23:59]
	FROM ree.Precios
	GROUP BY YEAR(Fecha)
	
	UNION ALL
	
	SELECT
		 YEAR(Fecha) AS Año
		,'MIN' AS TipoPrecio
		,MIN(IIF(Hora BETWEEN '00:00:00' AND '07:59:00', Valor_eur_mwh, NULL)) AS [00:00 - 07:59]
		,MIN(IIF(Hora BETWEEN '08:00:00' AND '09:59:00', Valor_eur_mwh, NULL)) AS [08:00 - 09:59]
		,MIN(IIF(Hora BETWEEN '10:00:00' AND '13:59:00', Valor_eur_mwh, NULL)) AS [10:00 - 13:59]
		,MIN(IIF(Hora BETWEEN '14:00:00' AND '17:59:00', Valor_eur_mwh, NULL)) AS [14:00 - 17:59]
		,MIN(IIF(Hora BETWEEN '18:00:00' AND '21:59:00', Valor_eur_mwh, NULL)) AS [18:00 - 21:59]
		,MIN(IIF(Hora BETWEEN '22:00:00' AND '23:59:00', Valor_eur_mwh, NULL)) AS [22:00 - 23:59]
	FROM ree.Precios
	GROUP BY YEAR(Fecha)
	) AS SegmentacionHoraria

ORDER BY Id DESC
;
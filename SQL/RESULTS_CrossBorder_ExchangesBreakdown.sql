SELECT
	 0 AS Id
	,'Total' AS Country
	,ROUND(SUM(IIF(Tipo = 'Exportación', Valor_Mwh, NULL)), 2) AS Export
	,ROUND(SUM(IIF(Tipo = 'Importación', Valor_Mwh, NULL)), 2) AS Import
	,ROUND(SUM(IIF(Tipo = 'Saldo', Valor_Mwh, NULL)), 2) AS Balance
FROM Intercambios

UNION ALL

SELECT
	 ROW_NUMBER() OVER (ORDER BY SUM(IIF(Tipo = 'Saldo', Valor_Mwh, NULL)) ASC)
	,Pais
	,ROUND(SUM(IIF(Tipo = 'Exportación', Valor_Mwh, 0)), 2)
	,ROUND(SUM(IIF(Tipo = 'Importación', Valor_Mwh, 0)), 2)
	,ROUND(SUM(IIF(Tipo = 'Saldo', Valor_Mwh, 0)), 2)
FROM Intercambios
GROUP BY Pais
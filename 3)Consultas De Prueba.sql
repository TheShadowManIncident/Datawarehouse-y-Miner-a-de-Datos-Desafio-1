
/*
Consultas de pruebas rapidas
*/

SET NOCOUNT ON;

/* ---------------------------------------------------------------
   1) Total de ventas por CLIENTE
----------------------------------------------------------------*/+
DECLARE @From INT 
DECLARE @To   INT
SELECT
  dc.CustomerId,
  dc.FirstName,
  dc.LastName,
  dc.Country,
  COUNT(DISTINCT fs.InvoiceId)          AS Invoices,
  SUM(fs.Quantity)                      AS Units,
  CAST(SUM(fs.LineTotal) AS NUMERIC(14,2)) AS Amount
FROM dw.FactSales fs
JOIN dw.DimCustomer dc ON dc.CustomerKey = fs.CustomerKey
GROUP BY dc.CustomerId, dc.FirstName, dc.LastName, dc.Country;

/* Uso:
-- SELECT * FROM dw.vw_TotalVentasPorCliente ORDER BY Amount DESC;
-- Con filtro de fechas:
-- SELECT c.*
-- FROM dw.vw_TotalVentasPorCliente c
-- JOIN dw.FactSales fs ON fs.CustomerKey = (SELECT CustomerKey FROM dw.DimCustomer WHERE CustomerId = c.CustomerId)
-- JOIN dw.DimDate d ON d.DateKey = fs.DateKey
-- WHERE d.[Date] BETWEEN '2009-01-01' AND '2010-12-31'
-- GROUP BY c.CustomerId, c.FirstName, c.LastName, c.Country
-- ORDER BY SUM(fs.LineTotal) DESC;
*/

/* ---------------------------------------------------------------
   2) Total de ventas por GÉNERO MUSICAL
----------------------------------------------------------------*/
DECLARE @From INT 
DECLARE @To   INT
SELECT
  dt.GenreName,
  SUM(fs.Quantity)                       AS Units,
  CAST(SUM(fs.LineTotal) AS NUMERIC(14,2)) AS Amount
FROM dw.FactSales fs
JOIN dw.DimTrack dt ON dt.TrackKey = fs.TrackKey
GROUP BY dt.GenreName;

/* Uso:
-- SELECT * FROM dw.vw_TotalVentasPorGenero ORDER BY Amount DESC;
-- Con filtro de periodo:
-- SELECT dt.GenreName, SUM(fs.LineTotal) AS Amount
-- FROM dw.FactSales fs
-- JOIN dw.DimTrack dt ON dt.TrackKey = fs.TrackKey
-- JOIN dw.DimDate d ON d.DateKey = fs.DateKey
-- WHERE d.[Year] = 2010
-- GROUP BY dt.GenreName
-- ORDER BY Amount DESC;
*/

/* ---------------------------------------------------------------
   3) Total de ventas por ARTISTA
----------------------------------------------------------------*/
DECLARE @From INT 
DECLARE @To   INT
SELECT
  dt.ArtistName,
  SUM(fs.Quantity)                       AS Units,
  CAST(SUM(fs.LineTotal) AS NUMERIC(14,2)) AS Amount
FROM dw.FactSales fs
JOIN dw.DimTrack dt ON dt.TrackKey = fs.TrackKey
GROUP BY dt.ArtistName;

/* Uso:
-- SELECT * FROM dw.vw_TotalVentasPorArtista ORDER BY Amount DESC;
-- Por año:
-- SELECT dt.ArtistName, SUM(fs.LineTotal) AS Amount
-- FROM dw.FactSales fs
-- JOIN dw.DimTrack dt ON dt.TrackKey = fs.TrackKey
-- JOIN dw.DimDate d ON d.DateKey = fs.DateKey
-- WHERE d.[Year] = 2010
-- GROUP BY dt.ArtistName
-- ORDER BY Amount DESC;
*/

/* ---------------------------------------------------------------
   4) Total de ventas por PAÍS
   (a) País del Cliente
   (b) País de Facturación (dirección en la factura)
----------------------------------------------------------------*/
DECLARE @From INT 
DECLARE @To   INT
SELECT
  dc.Country,
  SUM(fs.Quantity)                       AS Units,
  CAST(SUM(fs.LineTotal) AS NUMERIC(14,2)) AS Amount
FROM dw.FactSales fs
JOIN dw.DimCustomer dc ON dc.CustomerKey = fs.CustomerKey
GROUP BY dc.Country;

/* Uso:
-- SELECT * FROM dw.vw_TotalVentasPorPais_Cliente ORDER BY Amount DESC;
-- SELECT * FROM dw.vw_TotalVentasPorPais_Facturacion ORDER BY Amount DESC;
-- Filtro por rango de fechas:
-- SELECT Country, SUM(fs.LineTotal) AS Amount
-- FROM dw.FactSales fs
-- JOIN dw.DimBillingLocation dbl ON dbl.BillingLocationKey = fs.BillingLocationKey
-- JOIN dw.DimDate d ON d.DateKey = fs.DateKey
-- WHERE d.[Date] BETWEEN '2009-01-01' AND '2010-12-31'
-- GROUP BY Country
-- ORDER BY Amount DESC;
*/


PRINT('Analytics views created successfully.');

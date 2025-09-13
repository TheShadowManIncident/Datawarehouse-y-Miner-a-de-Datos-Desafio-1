/*
Datawarehouse y Minería de Datos DMD941 G01T (Virtual) Rene Alexis Barahona Bonilla BB241958
Procederemos a ejecutar los comandos SQL con los que vamos a desarrollar el modelo estrella teniendo en cuenta que se van a realizar limpiezas, validaciones y conversiones 
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

/* ------------------------------------------------------------------
  Este comando sirve para verificar que la Data extraida de la base Chinook existe y es correcta
-------------------------------------------------------------------*/
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='Employee'   AND schema_id = SCHEMA_ID('dbo')) RAISERROR('Missing table dbo.Employee',16,1);
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='Customer'   AND schema_id = SCHEMA_ID('dbo')) RAISERROR('Missing table dbo.Customer',16,1);
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='Invoice'    AND schema_id = SCHEMA_ID('dbo')) RAISERROR('Missing table dbo.Invoice',16,1);
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='InvoiceLine'AND schema_id = SCHEMA_ID('dbo')) RAISERROR('Missing table dbo.InvoiceLine',16,1);
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='Track'      AND schema_id = SCHEMA_ID('dbo')) RAISERROR('Missing table dbo.Track',16,1);
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='Album'      AND schema_id = SCHEMA_ID('dbo')) RAISERROR('Missing table dbo.Album',16,1);
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='Artist'     AND schema_id = SCHEMA_ID('dbo')) RAISERROR('Missing table dbo.Artist',16,1);
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='Genre'      AND schema_id = SCHEMA_ID('dbo')) RAISERROR('Missing table dbo.Genre',16,1);
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='MediaType'  AND schema_id = SCHEMA_ID('dbo')) RAISERROR('Missing table dbo.MediaType',16,1);

/* ------------------------------------------------------------------
   Seccion de preparacion para el proceso ETL 
-------------------------------------------------------------------*/
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='stg') EXEC('CREATE SCHEMA stg;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='dw')  EXEC('CREATE SCHEMA dw;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='qa')  EXEC('CREATE SCHEMA qa;');

/* ------------------------------------------------------------------
   Aca iniciacion un registro de rechazos para poder asegurarnos de una transferencia de datos limpia
-------------------------------------------------------------------*/
IF OBJECT_ID('stg.RowReject','U') IS NOT NULL DROP TABLE stg.RowReject;
CREATE TABLE stg.RowReject(
  RowRejectId INT IDENTITY(1,1) PRIMARY KEY,
  SourceTable  sysname,
  SourceId     INT        NULL,
  Issue        NVARCHAR(200),
  CapturedAt   DATETIME2  DEFAULT SYSDATETIME(),
  RawJson      NVARCHAR(MAX) NULL
);

/* ------------------------------------------------------------------
   Etapa de preparacion de la informacion 
-------------------------------------------------------------------*/
IF OBJECT_ID('stg.Customer','U') IS NOT NULL DROP TABLE stg.Customer;
SELECT
  c.CustomerId,
  TRIM(c.FirstName)    AS FirstName,
  TRIM(c.LastName)     AS LastName,
  NULLIF(TRIM(c.Company),'')    AS Company,
  NULLIF(TRIM(c.Address),'')    AS Address,
  NULLIF(TRIM(c.City),'')       AS City,
  NULLIF(TRIM(c.State),'')      AS [State],
  UPPER(TRIM(c.Country))        AS Country,
  NULLIF(TRIM(c.PostalCode),'') AS PostalCode,
  NULLIF(TRIM(c.Phone),'')      AS Phone,
  TRIM(c.Email)                 AS Email,
  c.SupportRepId
INTO stg.Customer
FROM dbo.Customer c;

IF OBJECT_ID('stg.Track','U') IS NOT NULL DROP TABLE stg.Track;
SELECT
  t.TrackId,
  TRIM(t.Name)               AS TrackName,
  NULLIF(TRIM(t.Composer),'') AS Composer,
  TRY_CONVERT(INT,t.Milliseconds)        AS Milliseconds,
  TRY_CONVERT(INT,t.Bytes)               AS Bytes,
  TRY_CONVERT(NUMERIC(10,2),t.UnitPrice) AS UnitPriceList,
  TRIM(a.Title)              AS AlbumTitle,
  TRIM(ar.Name)              AS ArtistName,
  TRIM(g.Name)               AS GenreName,
  TRIM(mt.Name)              AS MediaTypeName
INTO stg.Track
FROM dbo.Track t
LEFT JOIN dbo.Album a      ON a.AlbumId = t.AlbumId
LEFT JOIN dbo.Artist ar    ON ar.ArtistId = a.ArtistId
LEFT JOIN dbo.Genre g      ON g.GenreId = t.GenreId
LEFT JOIN dbo.MediaType mt ON mt.MediaTypeId = t.MediaTypeId;

IF OBJECT_ID('stg.Invoice','U') IS NOT NULL DROP TABLE stg.Invoice;
SELECT
  i.InvoiceId,
  i.CustomerId,
  CAST(i.InvoiceDate AS DATE) AS InvoiceDate,
  NULLIF(TRIM(i.BillingAddress),'')    AS BillingAddress,
  NULLIF(TRIM(i.BillingCity),'')       AS BillingCity,
  NULLIF(TRIM(i.BillingState),'')      AS BillingState,
  UPPER(TRIM(i.BillingCountry))        AS BillingCountry,
  NULLIF(TRIM(i.BillingPostalCode),'') AS BillingPostalCode,
  TRY_CONVERT(NUMERIC(10,2),i.Total)   AS InvoiceTotal
INTO stg.Invoice
FROM dbo.Invoice i;

IF OBJECT_ID('stg.InvoiceLine','U') IS NOT NULL DROP TABLE stg.InvoiceLine;
SELECT
  il.InvoiceLineId,
  il.InvoiceId,
  il.TrackId,
  TRY_CONVERT(NUMERIC(10,2),il.UnitPrice) AS UnitPrice,
  TRY_CONVERT(INT,il.Quantity)            AS Quantity
INTO stg.InvoiceLine
FROM dbo.InvoiceLine il;

/*Corroboracion de calidad de informacion */
INSERT INTO stg.RowReject(SourceTable,SourceId,Issue,RawJson)
SELECT 'InvoiceLine', il.InvoiceLineId, 'Quantity<=0 or UnitPrice<0',
       CONCAT('{"Quantity":',il.Quantity,',"UnitPrice":',il.UnitPrice,'}')
FROM stg.InvoiceLine il
WHERE ISNULL(il.Quantity,0) <= 0 OR ISNULL(il.UnitPrice,0) < 0;

DELETE il
FROM stg.InvoiceLine il
WHERE ISNULL(il.Quantity,0) <= 0 OR ISNULL(il.UnitPrice,0) < 0;

/* Integridad referencial: invoice/track */
INSERT INTO stg.RowReject(SourceTable,SourceId,Issue)
SELECT 'InvoiceLine', il.InvoiceLineId, 'Missing parent (Invoice or Track)'
FROM stg.InvoiceLine il
LEFT JOIN stg.Invoice si ON si.InvoiceId=il.InvoiceId
LEFT JOIN stg.Track   st ON st.TrackId=il.TrackId
WHERE si.InvoiceId IS NULL OR st.TrackId IS NULL;

DELETE il
FROM stg.InvoiceLine il
LEFT JOIN stg.Invoice si ON si.InvoiceId=il.InvoiceId
LEFT JOIN stg.Track   st ON st.TrackId=il.TrackId
WHERE si.InvoiceId IS NULL OR st.TrackId IS NULL;

/* Predeterminados para null money/price */
UPDATE stg.Invoice SET InvoiceTotal = 0 WHERE InvoiceTotal IS NULL;
UPDATE stg.InvoiceLine SET UnitPrice = 0 WHERE UnitPrice IS NULL;

/* ------------------------------------------------------------------
 Creacion de tablas de dimension y hechos
-------------------------------------------------------------------*/
IF OBJECT_ID('dw.FactSales','U') IS NOT NULL DROP TABLE dw.FactSales;
IF OBJECT_ID('dw.DimTrack','U') IS NOT NULL DROP TABLE dw.DimTrack;
IF OBJECT_ID('dw.DimBillingLocation','U') IS NOT NULL DROP TABLE dw.DimBillingLocation;
IF OBJECT_ID('dw.DimCustomer','U') IS NOT NULL DROP TABLE dw.DimCustomer;
IF OBJECT_ID('dw.DimEmployee','U') IS NOT NULL DROP TABLE dw.DimEmployee;
IF OBJECT_ID('dw.DimDate','U') IS NOT NULL DROP TABLE dw.DimDate;

CREATE TABLE dw.DimDate(
    DateKey       INT        NOT NULL PRIMARY KEY, -- yyyymmdd
    [Date]        DATE       NOT NULL UNIQUE,
    [Year]        SMALLINT   NOT NULL,
    [Quarter]     TINYINT    NOT NULL,
    [Month]       TINYINT    NOT NULL,
    [Day]         TINYINT    NOT NULL,
    MonthName     NVARCHAR(15),
    DayName       NVARCHAR(15),
    WeekOfYear    TINYINT
);

CREATE TABLE dw.DimEmployee(
    EmployeeKey   INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeId    INT NOT NULL,   -- NK
    FirstName     NVARCHAR(20) NOT NULL,
    LastName      NVARCHAR(20) NOT NULL,
    Title         NVARCHAR(30),
    City          NVARCHAR(40),
    State         NVARCHAR(40),
    Country       NVARCHAR(40),
    Email         NVARCHAR(60)
);
CREATE UNIQUE INDEX UX_DimEmployee_NK ON dw.DimEmployee(EmployeeId);

CREATE TABLE dw.DimCustomer(
    CustomerKey   INT IDENTITY(1,1) PRIMARY KEY,
    CustomerId    INT NOT NULL,   -- NK
    FirstName     NVARCHAR(40) NOT NULL,
    LastName      NVARCHAR(20) NOT NULL,
    Company       NVARCHAR(80),
    Address       NVARCHAR(70),
    City          NVARCHAR(40),
    [State]       NVARCHAR(40),
    Country       NVARCHAR(40),
    PostalCode    NVARCHAR(10),
    Phone         NVARCHAR(24),
    Email         NVARCHAR(60) NOT NULL,
    SupportRepId  INT NULL,       -- NK hacia Employee
    SupportRepName NVARCHAR(60) NULL
);
CREATE UNIQUE INDEX UX_DimCustomer_NK ON dw.DimCustomer(CustomerId);

CREATE TABLE dw.DimBillingLocation(
    BillingLocationKey INT IDENTITY(1,1) PRIMARY KEY,
    BillingAddress     NVARCHAR(70),
    BillingCity        NVARCHAR(40),
    BillingState       NVARCHAR(40),
    BillingCountry     NVARCHAR(40),
    BillingPostalCode  NVARCHAR(10)
);
CREATE UNIQUE INDEX UX_DimBilling_UQ ON dw.DimBillingLocation(BillingAddress, BillingCity, BillingState, BillingCountry, BillingPostalCode);

CREATE TABLE dw.DimTrack(
    TrackKey       INT IDENTITY(1,1) PRIMARY KEY,
    TrackId        INT NOT NULL,   -- NK
    TrackName      NVARCHAR(200) NOT NULL,
    Composer       NVARCHAR(220),
    Milliseconds   INT NOT NULL,
    Bytes          INT,
    UnitPriceList  NUMERIC(10,2) NOT NULL,
    AlbumTitle     NVARCHAR(160),
    ArtistName     NVARCHAR(120),
    GenreName      NVARCHAR(120),
    MediaTypeName  NVARCHAR(120)
);
CREATE UNIQUE INDEX UX_DimTrack_NK ON dw.DimTrack(TrackId);

CREATE TABLE dw.FactSales(
    FactSalesId        BIGINT IDENTITY(1,1) PRIMARY KEY,
    DateKey            INT        NOT NULL FOREIGN KEY REFERENCES dw.DimDate(DateKey),
    CustomerKey        INT        NOT NULL FOREIGN KEY REFERENCES dw.DimCustomer(CustomerKey),
    EmployeeKey        INT        NULL     FOREIGN KEY REFERENCES dw.DimEmployee(EmployeeKey),
    BillingLocationKey INT        NULL     FOREIGN KEY REFERENCES dw.DimBillingLocation(BillingLocationKey),
    TrackKey           INT        NOT NULL FOREIGN KEY REFERENCES dw.DimTrack(TrackKey),
    InvoiceId          INT        NOT NULL, -- degenerate
    InvoiceLineId      INT        NOT NULL, -- degenerate
    Quantity           INT        NOT NULL,
    UnitPrice          NUMERIC(10,2) NOT NULL,
    LineTotal          AS CAST(Quantity * UnitPrice AS NUMERIC(12,2)) PERSISTED
);
CREATE INDEX IX_FactSales_Date ON dw.FactSales(DateKey);
CREATE INDEX IX_FactSales_Customer ON dw.FactSales(CustomerKey);
CREATE INDEX IX_FactSales_Track ON dw.FactSales(TrackKey);

/* ------------------------------------------------------------------
  Carga de dimensiones (Type-1 upserts)
-------------------------------------------------------------------*/
MERGE dw.DimEmployee AS T
USING dbo.Employee AS S
   ON T.EmployeeId=S.EmployeeId
WHEN MATCHED THEN UPDATE SET
  T.FirstName=S.FirstName, T.LastName=S.LastName, T.Title=S.Title,
  T.City=S.City, T.State=S.State, T.Country=S.Country, T.Email=S.Email
WHEN NOT MATCHED BY TARGET THEN
  INSERT(EmployeeId,FirstName,LastName,Title,City,State,Country,Email)
  VALUES(S.EmployeeId,S.FirstName,S.LastName,S.Title,S.City,S.State,S.Country,S.Email);

MERGE dw.DimCustomer AS T
USING (
  SELECT c.*, CONCAT(e.FirstName,' ',e.LastName) AS SupportRepName
  FROM stg.Customer c
  LEFT JOIN dbo.Employee e ON e.EmployeeId=c.SupportRepId
) AS S
  ON T.CustomerId=S.CustomerId
WHEN MATCHED THEN UPDATE SET
  T.FirstName=S.FirstName, T.LastName=S.LastName, T.Company=S.Company,
  T.Address=S.Address, T.City=S.City, T.[State]=S.[State], T.Country=S.Country,
  T.PostalCode=S.PostalCode, T.Phone=S.Phone, T.Email=S.Email,
  T.SupportRepId=S.SupportRepId, T.SupportRepName=S.SupportRepName
WHEN NOT MATCHED BY TARGET THEN
  INSERT(CustomerId,FirstName,LastName,Company,Address,City,[State],Country,PostalCode,Phone,Email,SupportRepId,SupportRepName)
  VALUES(S.CustomerId,S.FirstName,S.LastName,S.Company,S.Address,S.City,S.[State],S.Country,S.PostalCode,S.Phone,S.Email,S.SupportRepId,S.SupportRepName);

MERGE dw.DimBillingLocation AS T
USING (
  SELECT DISTINCT BillingAddress,BillingCity,BillingState,BillingCountry,BillingPostalCode
  FROM stg.Invoice
) AS S
ON T.BillingAddress=S.BillingAddress
AND ISNULL(T.BillingCity,'')=ISNULL(S.BillingCity,'')
AND ISNULL(T.BillingState,'')=ISNULL(S.BillingState,'')
AND T.BillingCountry=S.BillingCountry
AND ISNULL(T.BillingPostalCode,'')=ISNULL(S.BillingPostalCode,'')
WHEN NOT MATCHED BY TARGET THEN
INSERT(BillingAddress,BillingCity,BillingState,BillingCountry,BillingPostalCode)
VALUES(S.BillingAddress,S.BillingCity,S.BillingState,S.BillingCountry,S.BillingPostalCode);

MERGE dw.DimTrack AS T
USING stg.Track AS S
  ON T.TrackId=S.TrackId
WHEN MATCHED THEN UPDATE SET
  T.TrackName=S.TrackName, T.Composer=S.Composer, T.Milliseconds=ISNULL(S.Milliseconds,0),
  T.Bytes=S.Bytes, T.UnitPriceList=ISNULL(S.UnitPriceList,0),
  T.AlbumTitle=S.AlbumTitle, T.ArtistName=S.ArtistName,
  T.GenreName=S.GenreName, T.MediaTypeName=S.MediaTypeName
WHEN NOT MATCHED BY TARGET THEN
  INSERT(TrackId,TrackName,Composer,Milliseconds,Bytes,UnitPriceList,AlbumTitle,ArtistName,GenreName,MediaTypeName)
  VALUES(S.TrackId,S.TrackName,S.Composer,ISNULL(S.Milliseconds,0),S.Bytes,ISNULL(S.UnitPriceList,0),S.AlbumTitle,S.ArtistName,S.GenreName,S.MediaTypeName);

/* DimDate from stg.Invoice range */
IF NOT EXISTS (SELECT 1 FROM dw.DimDate)
BEGIN
;WITH D AS (
  SELECT MIN(InvoiceDate) d0, MAX(InvoiceDate) d1 FROM stg.Invoice
),
Cal AS (
  SELECT d0 AS [Date], d1 FROM D
  UNION ALL SELECT DATEADD(DAY,1,[Date]), d1 FROM Cal WHERE [Date] < d1
)
INSERT INTO dw.DimDate(DateKey,[Date],[Year],[Quarter],[Month],[Day],MonthName,DayName,WeekOfYear)
SELECT CONVERT(INT,FORMAT([Date],'yyyyMMdd')),[Date],
       DATEPART(YEAR,[Date]),DATEPART(QUARTER,[Date]),DATEPART(MONTH,[Date]),DATEPART(DAY,[Date]),
       DATENAME(MONTH,[Date]), DATENAME(WEEKDAY,[Date]), DATEPART(WEEK,[Date])
FROM Cal OPTION (MAXRECURSION 0);
END;

/* ------------------------------------------------------------------
   Carga de FactSales
-------------------------------------------------------------------*/
INSERT INTO dw.FactSales
(DateKey,CustomerKey,EmployeeKey,BillingLocationKey,TrackKey,InvoiceId,InvoiceLineId,Quantity,UnitPrice)
SELECT
  CONVERT(INT,FORMAT(i.InvoiceDate,'yyyyMMdd'))             AS DateKey,
  dc.CustomerKey,
  de.EmployeeKey,
  dbl.BillingLocationKey,
  dt.TrackKey,
  i.InvoiceId,
  il.InvoiceLineId,
  il.Quantity,
  il.UnitPrice
FROM stg.InvoiceLine il
JOIN stg.Invoice i ON i.InvoiceId=il.InvoiceId
JOIN dw.DimCustomer dc ON dc.CustomerId=i.CustomerId
LEFT JOIN dbo.Employee e ON e.EmployeeId=dc.SupportRepId
LEFT JOIN dw.DimEmployee de ON de.EmployeeId=e.EmployeeId
LEFT JOIN dw.DimBillingLocation dbl
  ON dbl.BillingAddress     = i.BillingAddress
 AND ISNULL(dbl.BillingCity,'')  = ISNULL(i.BillingCity,'')
 AND ISNULL(dbl.BillingState,'') = ISNULL(i.BillingState,'')
 AND dbl.BillingCountry     = i.BillingCountry
 AND ISNULL(dbl.BillingPostalCode,'')=ISNULL(i.BillingPostalCode,'')
JOIN dw.DimTrack dt ON dt.TrackId=il.TrackId;

/* ------------------------------------------------------------------
    Agregados (materialized)
-------------------------------------------------------------------*/
IF OBJECT_ID('dw.AggCustomerSales','U') IS NOT NULL DROP TABLE dw.AggCustomerSales;
CREATE TABLE dw.AggCustomerSales(
  CustomerKey     INT NOT NULL PRIMARY KEY,       -- FK a DimCustomer
  CustomerId      INT NOT NULL,                   -- NK (for convenience)
  FirstInvoice    DATE NULL,
  LastInvoice     DATE NULL,
  InvoiceCount    INT  NOT NULL,
  LineCount       INT  NOT NULL,
  Units           INT  NOT NULL,
  Amount          NUMERIC(14,2) NOT NULL,         -- total spent by customer
  AvgTicket       NUMERIC(14,2) NULL
);

IF OBJECT_ID('dw.AggSalesByGenre','U') IS NOT NULL DROP TABLE dw.AggSalesByGenre;
CREATE TABLE dw.AggSalesByGenre(
  GenreName NVARCHAR(120) NOT NULL PRIMARY KEY,
  Units     INT NOT NULL,
  Amount    NUMERIC(14,2) NOT NULL
);

IF OBJECT_ID('dw.AggSalesByArtist','U') IS NOT NULL DROP TABLE dw.AggSalesByArtist;
CREATE TABLE dw.AggSalesByArtist(
  ArtistName NVARCHAR(120) NOT NULL PRIMARY KEY,
  Units      INT NOT NULL,
  Amount     NUMERIC(14,2) NOT NULL
);

IF OBJECT_ID('dw.AggSalesByCustomerCountry','U') IS NOT NULL DROP TABLE dw.AggSalesByCustomerCountry;
CREATE TABLE dw.AggSalesByCustomerCountry(
  Country NVARCHAR(40) NOT NULL PRIMARY KEY,
  Units   INT NOT NULL,
  Amount  NUMERIC(14,2) NOT NULL
);

;WITH PerInvoice AS (
  SELECT
    fs.CustomerKey, fs.InvoiceId,
    CAST(MIN(d.[Date]) AS DATE)            AS InvoiceDate,
    SUM(fs.Quantity)                       AS UnitsByInvoice,
    SUM(fs.LineTotal)                      AS AmountByInvoice
  FROM dw.FactSales fs
  JOIN dw.DimDate d ON d.DateKey = fs.DateKey
  GROUP BY fs.CustomerKey, fs.InvoiceId
)
INSERT INTO dw.AggCustomerSales(CustomerKey, CustomerId, FirstInvoice, LastInvoice, InvoiceCount, LineCount, Units, Amount, AvgTicket)
SELECT
  dc.CustomerKey,
  dc.CustomerId,
  MIN(pi.InvoiceDate)                                       AS FirstInvoice,
  MAX(pi.InvoiceDate)                                       AS LastInvoice,
  COUNT(*)                                                  AS InvoiceCount,
  SUM(pi.UnitsByInvoice)                                    AS LineCount,
  SUM(pi.UnitsByInvoice)                                    AS Units,
  CAST(SUM(pi.AmountByInvoice) AS NUMERIC(14,2))            AS Amount,
  CAST(AVG(pi.AmountByInvoice) AS NUMERIC(14,2))            AS AvgTicket
FROM PerInvoice pi
JOIN dw.DimCustomer dc ON dc.CustomerKey = pi.CustomerKey
GROUP BY dc.CustomerKey, dc.CustomerId;

INSERT INTO dw.AggSalesByGenre(GenreName, Units, Amount)
SELECT dt.GenreName,
       SUM(fs.Quantity)                AS Units,
       CAST(SUM(fs.LineTotal) AS NUMERIC(14,2)) AS Amount
FROM dw.FactSales fs
JOIN dw.DimTrack dt ON dt.TrackKey = fs.TrackKey
GROUP BY dt.GenreName;

INSERT INTO dw.AggSalesByArtist(ArtistName, Units, Amount)
SELECT dt.ArtistName,
       SUM(fs.Quantity)                AS Units,
       CAST(SUM(fs.LineTotal) AS NUMERIC(14,2)) AS Amount
FROM dw.FactSales fs
JOIN dw.DimTrack dt ON dt.TrackKey = fs.TrackKey
GROUP BY dt.ArtistName;

INSERT INTO dw.AggSalesByCustomerCountry(Country, Units, Amount)
SELECT dc.Country,
       SUM(fs.Quantity)                AS Units,
       CAST(SUM(fs.LineTotal) AS NUMERIC(14,2)) AS Amount
FROM dw.FactSales fs
JOIN dw.DimCustomer dc ON dc.CustomerKey = fs.CustomerKey
GROUP BY dc.Country;

/* ------------------------------------------------------------------
   (Inpeccion calidad) QA checks (non-fatal; inspect outputs)
-------------------------------------------------------------------*/
-- a) counts
SELECT (SELECT COUNT(*) FROM dbo.InvoiceLine) AS SrcLines,
       (SELECT COUNT(*) FROM dw.FactSales)    AS FactLines;

-- b) global reconciliation (detail sums)
SELECT 
  (SELECT SUM(CAST(Quantity*UnitPrice AS NUMERIC(12,2))) FROM dbo.InvoiceLine) AS SrcDetailTotal,
  (SELECT SUM(LineTotal) FROM dw.FactSales)                                     AS FactDetailTotal;

-- c) per-invoice reconciliation (header vs detail) - any rows returned indicate differences > 1 cent
SELECT i.InvoiceId, i.Total AS HeaderTotal, SUM(fs.LineTotal) AS DetailTotal,
       i.Total - SUM(fs.LineTotal) AS Diff
FROM dbo.Invoice i
JOIN dw.FactSales fs ON fs.InvoiceId=i.InvoiceId
GROUP BY i.InvoiceId, i.Total
HAVING ABS(i.Total - SUM(fs.LineTotal)) > 0.01;

-- d) orphan checks
SELECT TOP(1) 'Missing DateKey' AS Issue
FROM dw.FactSales fs LEFT JOIN dw.DimDate dd ON dd.DateKey=fs.DateKey
WHERE dd.DateKey IS NULL
UNION ALL
SELECT TOP(1) 'Missing CustomerKey'
FROM dw.FactSales fs LEFT JOIN dw.DimCustomer dc ON dc.CustomerKey=fs.CustomerKey
WHERE dc.CustomerKey IS NULL
UNION ALL
SELECT TOP(1) 'Missing TrackKey'
FROM dw.FactSales fs LEFT JOIN dw.DimTrack dt ON dt.TrackKey=fs.TrackKey
WHERE dt.TrackKey IS NULL;

-- e) negative/zero measures
SELECT COUNT(*) AS BadMeasureRows
FROM dw.FactSales
WHERE Quantity <= 0 OR UnitPrice < 0;

-- f) quick views
SELECT TOP (10) * FROM dw.AggCustomerSales ORDER BY Amount DESC;
SELECT TOP (10) * FROM dw.AggSalesByGenre ORDER BY Amount DESC;
SELECT TOP (10) * FROM dw.AggSalesByArtist ORDER BY Amount DESC;
SELECT TOP (10) * FROM dw.AggSalesByCustomerCountry ORDER BY Amount DESC;

PRINT('DW ETL completed.');

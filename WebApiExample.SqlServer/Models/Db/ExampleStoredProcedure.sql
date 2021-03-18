-- Example query from https://www.essentialsql.com/adventure-works-bill-materials-subqueries/
-- created as a sproc with price range parameters
-- Make sure to create this sproc in your AdventureWorks SQL Server database
-- before using GET /getListSubcomponents/{listPriceMin}/{listPriceMax}

CREATE PROCEDURE dbo.ListSubcomponents
  @ListPriceMin money,
  @ListPriceMax money
AS
BEGIN
	SELECT P.ProductID,
		   P.Name,
		   P.ProductNumber,
		   P.FinishedGoodsFlag,
		   P.ListPrice
	FROM   Production.Product P
	WHERE  P.SellEndDate is NULL
		   AND P.DiscontinuedDate is NULL
		   AND EXISTS (SELECT 1
					   FROM  Production.BillOfMaterials BOM
					   WHERE P.ProductID = BOM.ComponentID
					  )
		   AND P.ListPrice >= @ListPriceMin
		   AND P.ListPrice <= @ListPriceMax
END;
GO
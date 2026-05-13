CREATE FUNCTION Security.securitypredicate(@Agent AS nvarchar(200))  
    RETURNS TABLE  
WITH SCHEMABINDING  
AS  
    RETURN SELECT 1 AS securitypredicate_result
WHERE @Agent = USER_NAME() OR USER_NAME() = 'fabric_admin@abhijeetmohanty125gmail.onmicrosoft.com';
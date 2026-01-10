

CREATE PROCEDURE [dbo].[AT_GETTRAZA]
	@FechaIni date,
	@FechaFin date,
	@CODART varchar(50),
	@LOTE varchar(50)
AS begin
    SET NOCOUNT ON;  
 
SELECT C.Fecha FechaDoc, C.TipoCont, C.Serie,
    CASE WHEN LEN(CONVERT(VARCHAR(MAX), C.NumDoc)) > 15 THEN 'XXXXXXXXXXXXXXX' ELSE CONVERT(bigint, C.NumDoc) END NumDoc,
       C.IdFacV IdDoc,
       C.CodCli Codigo, C.NomCli Nombre,
       A.CodAlm, A.DescAlm,
       C.Referencia,
	   C.IdTot,
'FACV' TipoDoc,
9 TipoDocNum,
       L.UnidadesStock Unidades, L.CodArt, L.Lote, L.FecCaduc, T.HAYFECCADUC, T.HAYUBICACION,
	   (SELECT COUNT(*) FROM AT_REGISTROS_REL WHERE IDPEDV = L.IDPEDV) HayAppccRel
FROM CabeFacV C WITH(NOLOCK) INNER JOIN LineFact L WITH(NOLOCK)
     ON C.IdFacV = L.IdFacV
     LEFT JOIN Articulo T WITH(NOLOCK) ON L.CodArt = T.CodArt
     LEFT JOIN Almacen A WITH(NOLOCK) ON L.CodAlm = A.CodAlm
WHERE (C.Fecha >= @Fechaini) AND
      (C.Fecha <= @FechaFin)
  AND (L.CodArt = @CODART)
  AND ( ( L.LOTE IN (@LOTE)))
UNION ALL
SELECT C.Fecha FechaDoc, C.TipoCont, C.Serie,
       CASE WHEN LEN(CONVERT(VARCHAR(MAX), C.NumDoc)) > 15 THEN 'XXXXXXXXXXXXXXX' ELSE CONVERT(bigint, C.NumDoc) END NumDoc,
       C.IdAlbV IdDoc,
       C.CodCli Codigo, C.NomCli Nombre,
       A.CodAlm, A.DescAlm,
       C.Referencia,
	   C.IdTot,
'ALBV' TipoDoc,
8 TipoDocNum,
       L.UnidadesStock Unidades, L.CodArt, L.Lote, L.FecCaduc, T.HAYFECCADUC, T.HAYUBICACION, (SELECT COUNT(*) FROM AT_REGISTROS_REL WHERE IDPEDV = L.IDPEDV) HayAppccRel
FROM CabeAlbV C WITH(NOLOCK) INNER JOIN LineAlba L WITH(NOLOCK)
     ON C.IdAlbV = L.IdAlbV
     LEFT JOIN Articulo T WITH(NOLOCK) ON L.CodArt = T.CodArt
     LEFT JOIN Almacen A WITH(NOLOCK) ON L.CodAlm = A.CodAlm
WHERE (C.Fecha >= @Fechaini) AND
      (C.Fecha <= @FechaFin)
  AND (L.CodArt = @CODART)
  AND ( ( L.LOTE IN (@LOTE))) and c.SITUACION='A'
UNION ALL
SELECT C.Fecha FechaDoc, C.TipoCont, C.Serie,
       CASE WHEN LEN(CONVERT(VARCHAR(MAX), C.NumDoc)) > 15 THEN 'XXXXXXXXXXXXXXX' ELSE CONVERT(bigint, C.NumDoc) END NumDoc,
       C.IdDepV IdDoc,
       C.CodCli Codigo, C.NomCli Nombre,
       A.CodAlm, A.DescAlm,
       C.Referencia,
	   C.IdTot,
'DEPV' TipoDoc,
10 TipoDocNum,
       L.UnidadesStock Unidades, L.CodArt, L.Lote, L.FecCaduc, T.HAYFECCADUC, T.HAYUBICACION, 0 HayAppccRel
FROM __CabeDepV C WITH(NOLOCK) INNER JOIN __LineDepo L WITH(NOLOCK)
     ON C.IdDepV = L.IdDepV
     LEFT JOIN Articulo T WITH(NOLOCK) ON L.CodArt = T.CodArt
     LEFT JOIN Almacen A WITH(NOLOCK) ON L.CodAlm = A.CodAlm
WHERE (C.Fecha >= @Fechaini) AND
      (C.Fecha <= @FechaFin)
  AND (L.CodArt = @CODART)
  AND ( ( L.LOTE IN (@LOTE)))
UNION ALL
SELECT C.Fecha FechaDoc, C.TipoCont, C.Serie,
       CASE WHEN LEN(CONVERT(VARCHAR(MAX), C.NumDoc)) > 15 THEN 'XXXXXXXXXXXXXXX' ELSE CONVERT(bigint, C.NumDoc) END NumDoc,
       C.IdAlbC IdDoc,
       C.CodPro Codigo, C.NomPro +'-'+ (select top 1 LTRIM(CODART) +'- L:'+ LTRIM(LOTE)  +'- C:'+ CONVERT(varchar(10),UNIDADES) from LINEALBA where IDALBC =C.IDALBC and UNIDADES > 0) Nombre,
       A.CodAlm, A.DescAlm,
       C.Referencia,
	   C.IdTot,
'ALBC' TipoDoc,
3 TipoDocNum,
       L.UnidadesStock Unidades, L.CodArt, L.Lote, L.FecCaduc, T.HAYFECCADUC, T.HAYUBICACION,  
	   (SELECT COUNT(*) FROM AT_REGISTROS_REL WHERE IDPEDC = L.IDPEDC) + 
	   (SELECT COUNT(*) FROM AT_REGISTROS_REL WHERE IDPEDV IN (SELECT IDENTHIJO FROM VINCULOS WHERE TIPO = 'CABEALBC' AND TIPOHIJO = 'CABEPEDV' AND IDENT = C.IDALBC)) +
	   (SELECT COUNT(*) FROM AT_REGISTROS_REL WHERE IDIDENTIFICADOR IN (SELECT IDIDENTIFICADOR FROM AT_IDENTIFICADORES_DET WHERE AT_IDPRODUCCION = C.PARAM9)) HayAppccRel

FROM CabeAlbC C WITH(NOLOCK) INNER JOIN LineAlba L WITH(NOLOCK)
     ON C.IdAlbC = L.IdAlbC
     LEFT JOIN Articulo T WITH(NOLOCK) ON L.CodArt = T.CodArt
     LEFT JOIN Almacen A WITH(NOLOCK) ON L.CodAlm = A.CodAlm

WHERE (C.Fecha >= @Fechaini) AND
      (C.Fecha <= @FechaFin)
  AND (L.CodArt = @CODART)
  AND ( ( L.LOTE IN (@LOTE)))
UNION ALL
SELECT C.Fecha FechaDoc, C.TipoCont, C.Serie,
       CASE WHEN LEN(CONVERT(VARCHAR(MAX), C.NumDoc)) > 15 THEN 'XXXXXXXXXXXXXXX' ELSE CONVERT(bigint, C.NumDoc) END NumDoc,
       C.IdDepC IdDoc,
       C.CodPro Codigo, C.NomPro Nombre,
       A.CodAlm, A.DescAlm,
       C.Referencia,
	   C.IdTot,
'DEPC' TipoDoc,
5 TipoDocNum,
       L.UnidadesStock Unidades, L.CodArt, L.Lote, L.FecCaduc, T.HAYFECCADUC, T.HAYUBICACION, 0 HayAppccRel
FROM __CabeDepC C WITH(NOLOCK) INNER JOIN __LineDepo L WITH(NOLOCK)
     ON C.IdDepC = L.IdDepC
     LEFT JOIN Articulo T WITH(NOLOCK) ON L.CodArt = T.CodArt
     LEFT JOIN Almacen A WITH(NOLOCK) ON L.CodAlm = A.CodAlm
WHERE (C.Fecha >= @Fechaini) AND
      (C.Fecha <= @FechaFin)
  AND (L.CodArt = @CODART)
  AND ( ( L.LOTE IN (@LOTE)))
UNION ALL
SELECT C.Fecha FechaDoc, '' TipoCont, C.Serie,
       CASE WHEN LEN(CONVERT(VARCHAR(MAX), C.NumDoc)) > 15 THEN 'XXXXXXXXXXXXXXX' ELSE CONVERT(bigint, C.NumDoc) END NumDoc,
       C.IdTra IdDoc,
       '' Codigo, '' Nombre,
       A.CodAlm, A.DescAlm,
       C.Motivo,
	   C.IdTot,
       'TRAS' TipoDoc,
       11 TipoDocNum,
       L.UnidadesStock Unidades, L.CodArt, L.Lote, L.FecCaduc, T.HAYFECCADUC, T.HAYUBICACION, 0 HayAppccRel
FROM CabeTras C WITH(NOLOCK) INNER JOIN LineTras L WITH(NOLOCK)
     ON C.IdTra = L.IdTra
     LEFT JOIN Articulo T WITH(NOLOCK) ON L.CodArt = T.CodArt
     LEFT JOIN Almacen A WITH(NOLOCK) ON L.CodAlmEnt = A.CodAlm
WHERE (C.Fecha >= @Fechaini) AND
      (C.Fecha <= @FechaFin)
  AND (L.CodArt = @CODART)
  AND ( ( L.LOTE IN (@LOTE)))
UNION ALL
SELECT C.Fecha FechaDoc, '' TipoCont, C.Serie,
       CASE WHEN LEN(CONVERT(VARCHAR(MAX), C.NumDoc)) > 15 THEN 'XXXXXXXXXXXXXXX' ELSE CONVERT(bigint, C.NumDoc) END NumDoc,
       C.IdTra IdDoc,
       '' Codigo, '' Nombre,
       A.CodAlm, A.DescAlm,
       C.Motivo,
	   C.IdTot,
       'TRAS' TipoDoc,
       12 TipoDocNum,
       -L.UnidadesStock Unidades, L.CodArt, L.Lote, L.FecCaduc, T.HAYFECCADUC, T.HAYUBICACION, 0 HayAppccRel
FROM CabeTras C WITH(NOLOCK) INNER JOIN LineTras L WITH(NOLOCK)
     ON C.IdTra = L.IdTra
     LEFT JOIN Articulo T WITH(NOLOCK) ON L.CodArt = T.CodArt
     LEFT JOIN Almacen A WITH(NOLOCK) ON L.CodAlmSal = A.CodAlm
WHERE (C.Fecha >= @Fechaini) AND
      (C.Fecha <= @FechaFin)
  AND (L.CodArt = @CODART)
  AND ( ( L.LOTE IN (@LOTE)))
UNION ALL
SELECT C.Fecha FechaDoc, '' TipoCont, C.Serie,
       CASE WHEN LEN(CONVERT(VARCHAR(MAX), C.NumDoc)) > 15 THEN 'XXXXXXXXXXXXXXX' ELSE CONVERT(bigint, C.NumDoc) END NumDoc,
       C.IdReg IdDoc,
       '' Codigo, '' Nombre,
       A.CodAlm, A.DescAlm,
       C.Motivo,
	   C.IdTot,
       'REGU' TipoDoc,
       13 TipoDocNum,
       L.UnidadesStock Unidades, L.CodArt, L.Lote, L.FecCaduc, T.HAYFECCADUC, T.HAYUBICACION, 0 HayAppccRel
FROM CabeRegu C WITH(NOLOCK) INNER JOIN LineRegu L WITH(NOLOCK)
     ON C.IdReg = L.IdReg
     LEFT JOIN Articulo T WITH(NOLOCK) ON L.CodArt = T.CodArt
     LEFT JOIN Almacen A WITH(NOLOCK) ON L.CodAlm = A.CodAlm
WHERE (C.Fecha >= @Fechaini) AND
      (C.Fecha <= @FechaFin)
  AND (L.CodArt = @CODART)
  AND ( ( L.LOTE IN (@LOTE)))
UNION ALL
SELECT C.Fecha FechaDoc, '' TipoCont, '' Serie,
       CONVERT(bigint, C.IdInven) NumDoc,
       C.IdInven IdDoc,
       '' Codigo, '' Nombre,
       A.CodAlm, A.DescAlm,
       C.Motivo,
	   C.IdTot,
       'INVE' TipoDoc,
       14 TipoDocNum,
       L.UnidadesStock Unidades, L.CodArt, L.Lote, L.FecCaduc, T.HAYFECCADUC, T.HAYUBICACION, 0 HayAppccRel
FROM CabeInve C WITH(NOLOCK) INNER JOIN LineInve L WITH(NOLOCK)
     ON C.IdInven = L.IdInven
     LEFT JOIN Articulo T WITH(NOLOCK) ON L.CodArt = T.CodArt
     LEFT JOIN Almacen A WITH(NOLOCK) ON L.CodAlm = A.CodAlm
WHERE (C.Fecha >= @Fechaini) AND
      (C.Fecha <= @FechaFin)
  AND (L.CodArt = @CODART)
  AND ( ( L.LOTE IN (@LOTE)))
ORDER BY L.CodArt,
Lote, FecCaduc,
         FechaDoc, TipoDocNum, TipoCont, Serie, NumDoc
		 end

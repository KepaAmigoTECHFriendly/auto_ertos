library(httr)
library(openxlsx)
library(RPostgres)
library(RPostgreSQL)
library(DBI)
library(dplyr)
library(lubridate)

mes_url <- ifelse(month(Sys.Date()) == 1,12,(month(Sys.Date())-1))
if(mes_url<10){
  url <- paste("observatoritreball.gencat.cat/web/.content/generic/documents/treball/estadistica/regulacio_d_ocupacio/2020/arxius/regulacio_ocupacio-20200",mes_url,".xlsx",sep = "")
}else{
  url <- paste("observatoritreball.gencat.cat/web/.content/generic/documents/treball/estadistica/regulacio_d_ocupacio/2020/arxius/regulacio_ocupacio-2020",mes_url,".xlsx",sep = "")
}

# Es necesario crear un archivo .xlsx vacio para poder descargar el archivo deseado sobre este
openxlsx::write.xlsx(NULL, file = paste(tempdir(),"/fichero.xlsx",sep = ""))

download.file(url,  paste(tempdir(),"/fichero.xlsx",sep = ""), mode = "wb")






nombre_hojas <- getSheetNames( paste(tempdir(),"/fichero.xlsx",sep = ""))
if(nombre_hojas > 13){
  # Carga datos
  df_tipo_expediente <- read.xlsx(xlsxFile =  paste(tempdir(),"/fichero.xlsx",sep = ""), sheet = grep("9",nombre_hojas)[1], skipEmptyRows = TRUE)
  df_expediente_econom <- read.xlsx(xlsxFile =  paste(tempdir(),"/fichero.xlsx",sep = ""), sheet = grep("10",nombre_hojas)[1], skipEmptyRows = TRUE)
  df_expediente_trabajo <- read.xlsx(xlsxFile =  paste(tempdir(),"/fichero.xlsx",sep = ""), sheet = grep("11",nombre_hojas)[1], skipEmptyRows = TRUE)
  
  periodo_mes <- seq(1,12,1)
  names(periodo_mes) <- c("Gener","Febrer","Març","Abril","Maig","Juny",
                          "Juliol","Agost","Setembre","Octubre","Novembre","Desembre")
  
  mes <- gsub(" [0-9].*","",df_tipo_expediente[1,1])
  ano <- gsub(".* ","",df_tipo_expediente[1,1])
  fecha <- paste(ano,as.numeric(periodo_mes[grep(mes,names(periodo_mes))]),"01",sep = "-")
  fecha <- as.Date(fecha)
  
  # Eliminar fila 1, informativa
  df_tipo_expediente <- df_tipo_expediente[-1,]
  df_expediente_econom <- df_expediente_econom[-1,]
  df_expediente_trabajo <- df_expediente_trabajo[-1,]
  
  # Asignación nombre columnas
  colnames(df_tipo_expediente) <- df_tipo_expediente[2,]
  colnames(df_expediente_econom) <- df_expediente_econom[1,]
  colnames(df_expediente_trabajo) <- df_expediente_trabajo[1,]
  
  colnames(df_tipo_expediente)[1] <- "Comarca"
  colnames(df_expediente_econom)[1] <- "Comarca"
  colnames(df_expediente_trabajo)[1] <- "Comarca"
  
  # Eliminar fila 1, es la misma que nombre columnas
  df_tipo_expediente <- df_tipo_expediente[-2,]
  df_expediente_econom <- df_expediente_econom[-1,]
  df_expediente_trabajo <- df_expediente_trabajo[-1,]
  
  # Trataimiento df expedientes
  colnames(df_tipo_expediente)[4:11] <- paste(colnames(df_tipo_expediente)[4:11],df_tipo_expediente[1,4],sep = " ")
  colnames(df_tipo_expediente)[12:ncol(df_tipo_expediente)] <- paste(colnames(df_tipo_expediente)[12:ncol(df_tipo_expediente)],df_tipo_expediente[1,12],sep = " ")
  df_tipo_expediente <- df_tipo_expediente[-1,]
  
  # Otros tratamientos
  for(i in 4:15){
    df_tipo_expediente[,i] <- as.numeric(df_tipo_expediente[,i])
  }
  df_tipo_expediente <- df_tipo_expediente[-grep("Catalunya",df_tipo_expediente$Comarca),]
  for(i in 4:26){
    df_expediente_econom[,i] <- as.numeric(df_expediente_econom[,i])
  }
  df_expediente_econom <- df_expediente_econom[-grep("Catalunya",df_expediente_econom$Comarca),]
  for(i in 4:26){
    df_expediente_trabajo[,i] <- as.numeric(df_expediente_trabajo[,i])
  }
  df_expediente_trabajo <- df_expediente_trabajo[-grep("Catalunya",df_expediente_trabajo$Comarca),]
  
  # ASIGNACIÓN FECHA
  df_tipo_expediente$fecha <- rep(fecha,nrow(df_tipo_expediente))
  df_expediente_econom$fecha <- rep(fecha,nrow(df_expediente_econom))
  df_expediente_trabajo$fecha <- rep(fecha,nrow(df_expediente_trabajo))
  
  
  # ====================================
  # Guardado en BBDD
  # ====================================
  db          <- 'amb'
  host_db     <- '94.130.26.60'
  db_port     <- '5432'
  db_user     <- 'postgres'
  db_password <- 'root_tech_2019'
  
  con <- dbConnect(RPostgres::Postgres(), dbname = db, host=host_db, port=db_port, user=db_user, password=db_password)
  
  # Escritura en tablas
  # Tabla 1 - Tipo expediente
  dbWriteTable(con, 'tipo_expediente_temporal',df_tipo_expediente, temporary = TRUE)
  consulta_evitar_duplicados <- 'INSERT INTO tipo_expediente SELECT * FROM tipo_expediente_temporal a WHERE NOT EXISTS (SELECT 0 FROM tipo_expediente b where b."Codi INE municipi" = a."Codi INE municipi" AND b.fecha = a.fecha)'
  dbGetQuery(con, consulta_evitar_duplicados)  # Ejecución consulta
  dbRemoveTable(con,"tipo_expediente_temporal")   # Eliminación tabla temporal
  #dbWriteTable(con, 'tipo_expediente',df_tipo_expediente, temporary = FALSE)
  
  # Tabla 2 - Expedientes por sección económica
  dbWriteTable(con, 'expediente_econom_temporal',df_expediente_econom, temporary = TRUE)
  consulta_evitar_duplicados <- 'INSERT INTO expediente_econom SELECT * FROM expediente_econom_temporal a WHERE NOT EXISTS (SELECT 0 FROM expediente_econom b where b."Codi INE municipi" = a."Codi INE municipi" AND b.fecha = a.fecha)'
  dbGetQuery(con, consulta_evitar_duplicados)  # Ejecución consulta
  dbRemoveTable(con,"expediente_econom_temporal")   # Eliminación tabla temporal
  #dbWriteTable(con, 'expediente_econom',df_expediente_econom, temporary = FALSE)
  
  # Tabla 3 - Trabajadores por sección económica
  dbWriteTable(con, 'trabajadores_econom_temporal',df_expediente_trabajo, temporary = TRUE)
  consulta_evitar_duplicados <- 'INSERT INTO trabajadores_econom SELECT * FROM trabajadores_econom_temporal a WHERE NOT EXISTS (SELECT 0 FROM trabajadores_econom b where b."Codi INE municipi" = a."Codi INE municipi" AND b.fecha = a.fecha)'
  dbGetQuery(con, consulta_evitar_duplicados)  # Ejecución consulta
  dbRemoveTable(con,"trabajadores_econom_temporal")   # Eliminación tabla temporal
  #dbWriteTable(con, 'trabajadores_econom',df_expediente_trabajo, temporary = FALSE)
}






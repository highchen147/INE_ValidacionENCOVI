# Inconsistencias de ocupación principal 
library(conflicted)
conflicted::conflict_prefer('filter','dplyr')

library(haven)
library(dplyr)
library(readxl)
library(writexl)

library(RMySQL)

rm(list = ls())
cat('\014')

ruta <- "Mario"
grupos <- "Grupos.xlsx"

sql <- 
"
SELECT * FROM visitas WHERE occ = -1
"

data <- NULL
ex <- tryCatch({
  cnn <- dbConnect(MySQL(), 
                   user = 'rrcastillo', 
                   password = 'Rcastillo2023', 
                   dbname = 'encabih', 
                   host = '20.10.8.4', 
                   port=3308)
  
  #Obtener datos de ocupaciones
  data <- dbGetQuery(cnn, sql)
  
}, finally = {
  dbDisconnect(cnn)
})

if (FALSE) {
  gs <- read_excel(file.path(ruta, grupos))
  data <- data %>% 
    left_join(gs, by = c("P01A02", "P01A03", "P01A04"))

  message(paste0("Total de inconsistencias: ", nrow(data)))
  
  # Crear la carpeta "Mario" si no existe
  if (!dir.exists("Mario")) {
    dir.create("Mario")
  }
  
  # Crear una carpeta con marca temporal
  timestamp <- format(Sys.time(), "%d-%m-%H-%M")
  timestamp_folder <- paste0("Mario/Inconsistencias_", timestamp)
  dir.create(timestamp_folder)
  
  # Guardar el archivo Excel de inconsistencias totales
  write_xlsx(data, file.path(timestamp_folder, "Inconsistencias.xlsx"))
  
  lista <- unique(data$GRUPO)
  for (item in lista) {
    cuadro <- data %>% 
      filter(GRUPO == item) %>%
      select(-c("VALOR", "GRUPO"))
    
    nombre <- paste0("InconsistenciasGRUPO", item, ".xlsx")
    
    message(paste0(">> ", nombre, " -> ", nrow(cuadro)))
    
    # Guardar el archivo Excel de inconsistencias por grupo
    write_xlsx(cuadro, file.path(timestamp_folder, nombre))
  }
  
} else {
  message("No se recuperó información")
}


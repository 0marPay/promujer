
from hubspot import establecer_conexion, get_data_for_create, create, insert, to_uderscore, delete_table
import pandas as pd

file = "Form Inscripcion 4taC - Fortalece Bolivia.csv"
file = "Form Inscripcion 4taC - Fortalece Bs.Aires.csv"
file = "Form Inscripcion 4taC - Fortalece Centroamerica.csv"
file = "Form Inscripcion 4taC - Fortalece Chile.csv"
file = "Form Inscripcion 4taC - Fortalece MP MX.csv"
file = "Form Inscripcion 4taC - Fortalece SudAmerica.csv"
file = "Form Inscripcion 4taC - Fortalece surestemx.csv"
file = "Form Inscripcion 4taC - Mejora Bolivia.csv"
file = "Form Inscripcion 4taC - Mejora Bs.Aires.csv"
file = "Form Inscripcion 4taC - Mejora Centroamerica.csv"
file = "Form Inscripcion 4taC - Mejora Chile.csv"
file = "Form Inscripcion 4taC - Mejora MP MX.csv"
file = "Form Inscripcion 4taC - Mejora SudAmerica.csv"
file = "Form Inscripcion 4taC - Mejora surestemx.csv"

file = "Formulario Comienza tu Negocio (1).csv"
file = "Formulario inscripcion 2022 - Emprende (3ra cohorte) (23 Mayo) (1).csv"
file = "Formulario inscripcion 2022 - Emprende PM y Mercado Pago.csv"
file = "Formulario inscripcion 2022 - Emprende y MP (3ra cohorte) (1).csv"
file = "Formulario inscripcion 2022- Emprende  (3ra cohorte) (1).csv"
file = "Formulario Mejora tu negocio (1).csv"
file = "Postulantes a la 2ra cohorte (convocatoria abierta).csv"

file = "hubspot-crm-exports-emprende-pro-mujer-2023-06-01.csv"

fixed_file = to_uderscore(file, added="hubspot_data ()")

def create_hubspot_data(my_file, data, connection, start_index): 
    # Obtenemos los datos del archivo csv
    # clean_data = get_data_for_create(data)
    # if "NombreDelEmprendimiento" in clean_data:
    #     del clean_data["NombreDelEmprendimiento"]
    clean_data = {"CorreoQueUsasteParaIngresarAEmprendeProMujer":234,"Pais2":125,"CualEsSuPaisDeResidencia":163,"Pais3":894,"TelefonoCelular2":1025,"DocumentoCurpCredito_Impulsate":571,"NumeroDeTelefonoDeWhatsapp":880,"NombreFirstName":702,"ApellidoLastName":766,"SeleccionePais":978,"CualEsTuDniCiDeIdentificacionPersonal":11,"NumeroTelefonoSinFotmato":885,"RecordId":0,"Apellido":2,"Curp_Promujer":528,"NumeroTelefonoDeCelular":357,"Nombre":1,"NombreFullName":716,"PaisDeLaIp":896,"CorreoElectronicoDelTrabajo":507,"Curp":527,"Paisregion":898,"CorreoElectronicoDelMiembro":506,"PaisDeResidencia2":372,"CorreoElectronico":504,"Pais":117,"Correo":503,"FechaDeNacimiento":315,"TelefonoNegocioCredito_Vivienda":1027,"DniDeDocumentoDeIdentidad":870,"CualEsSuPaisDeResidencia2":178,"NombreDeLaOrganizacion":855,"IdentificacionPersoonalDniCiCurp":318,"CarnetDeIdentidadDocumentoCedulaIneDniRfc":239,"DniDeIdentificacionPersonal":877,"IngreseSuCurpParaContinuar":747,"DireccionesDeCorreoElectronicoAdicionales":1144,"FechaDeNacimiento3":671,"Dni123":884,"PhoneNumberTelefono":904,"Dni":570,"FechaDeNacimiento2":670,"NumeroDeTelefono":879,"NroCelularTelefono":864,"DocumentoCurpCredito_Vivienda":572,"TelefonoNegocioCredito_Impulsate":1026,"NumeroDeCelular2Telefono":869,"FechaDeNacimientoDdmmaaaa":672,"Nombres":860,"DniDeDocumentoDeIdentidad2":871,"ConfirmacionDeTelefono":491,"NombreCompleto":850,"SeleccionElPais":972,"DocumentoDeIdentidadDniCiCurpcedulaEmprende":573,"PaisORegion":897,"DocumentoDeIdentidadCedulaIneDniRfc":334,"DocumentoDeTuCurp":574,"NoDeContactoTelefono":321,"TelefonoCelular":223,"Apellidos":434,"Rfc":960,"PaisDeResidencia":358,"CualEsTuPais":127,"NumeroDeWhatsappTelefono2":883, "NumeroTelefonicoTelefono":886,"Nombre2":124}
    columnas = sorted(clean_data, key=clean_data.get)
    # columnas = ["RecordId"] + columnas 
    # A partir de estos datos generamos una tabla sql
    my_file = to_uderscore(my_file, added="hubspot_data ()")
    name_tabla = f"`hubspot_data ({my_file})`"
    delete_table(name_tabla, connection)
    # revisamos si tiene alguna palabra para identicacion oficial y si es el caso le agregamos Dni
    identificadores = ["Curp", "Rfc", "Rut", "Identificacion"]
    columnas = [elem + "Dni" if any(identificador in elem for identificador in identificadores) else elem for elem in columnas]
    is_created = create(connection, columnas, name_tabla)

    if not is_created:
        return

    # Agregamos los valores 
    columnas_index = sorted(clean_data.values())
    data = data.iloc[:, columnas_index]
    # data.insert(0, 'Record ID', [i+ start_index for i, _ in enumerate(data['1. Nombres'])])
    data.reset_index(drop=True, inplace=True)
    insert(connection, columnas, data, name_tabla, bunch_size=10)

if __name__ == '__main__':
    
    print("Processing...")
    print(file)
    
    csv_file = f"C:/Users/olopezric/Documents/PROMUJER/hubspot/{file}"
    data = pd.read_csv(csv_file)
    print("filas totales:", data.shape[0])
    connection = establecer_conexion(database="hubpspot_db")
    # start_index = 0 + 132 + 17 + 818 + 57 + 1249 + 1823 + 156 + 289 + 25 + 2485 + 112 + 2270 + 4670 + 439
    # start_index = start_index + 112 + 188 + 787 + 3377 + 2865 + 100 + 385
    start_index = 0
    create_hubspot_data(file, data, connection, start_index)
    
    # Cerrar la conexión
    connection.close()
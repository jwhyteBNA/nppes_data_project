import azure.functions as func

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# add source connection fn (azurite)


# add target connection (postgres)


# download functions (azurite)
# paginating in chunks


@app.route(route="NPPES_Data_Cleaning")
def NPPES_Data_Cleaning(req: func.HttpRequest) -> func.HttpResponse:

    pass

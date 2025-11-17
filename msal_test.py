import msal

def get_token_device_code(client_id, tenant_id, scopes):
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.PublicClientApplication(client_id, authority=authority)

    flow = app.initiate_device_flow(scopes=scopes)
    if "user_code" not in flow:
        print("Failed to start device flow:", flow)
        return None

    print(flow["message"])
    result = app.acquire_token_by_device_flow(flow)

    # Debug: print full result
    print("Result of acquire_token_by_device_flow:", result)

    if "access_token" in result:
        return result["access_token"]
    else:
        print("Error:", result.get("error"))
        print("Error description:", result.get("error_description"))
        return None

if __name__ == "__main__":
    CLIENT_ID = "your-client-id-here"
    TENANT_ID = "your-tenant-id-here"
    SCOPES = ["User.Read"]

    token = get_token_device_code(CLIENT_ID, TENANT_ID, SCOPES)
    if token:
        print("Access Token:", token)

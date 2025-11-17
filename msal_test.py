import msal

def get_token_device_code(client_id, tenant_id, scopes):
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.PublicClientApplication(client_id, authority=authority)

    # First try to get a token from cache (if user has already signed in before)
    accounts = app.get_accounts()
    if accounts:
        result = app.acquire_token_silent(scopes, account=accounts[0])
    else:
        result = None

    if not result:
        # Initiate device code flow
        flow = app.initiate_device_flow(scopes=scopes)
        if "user_code" not in flow:
            raise ValueError("Failed to create device flow. Error: %s" % flow)
        print(flow["message"])  # Ask user to go to URL and enter code
        result = app.acquire_token_by_device_flow(flow)  # This will block

    if "access_token" in result:
        return result["access_token"]
    else:
        print("Error acquiring token:")
        print(result.get("error"))
        print(result.get("error_description"))
        print(result.get("correlation_id"))
        return None

if __name__ == "__main__":
    CLIENT_ID = "your-client-id"
    TENANT_ID = "your-tenant-id"
    SCOPES = ["User.Read"]  # adjust scope as needed

    token = get_token_device_code(CLIENT_ID, TENANT_ID, SCOPES)
    if token:
        print("Access token:", token)

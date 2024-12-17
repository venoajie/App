# -*- coding: utf-8 -*-

import oci
import base64


def get_oci_key () -> list:
     
    # Specify the OCID of the secret to retrieve
    keyOCID = "ocid1.vaultsecret.oc1.eu-frankfurt-1.amaaaaaaaenu5lyakmdxctv7twvwovaa2jakvkp7svkhnokhnzd5vp2xog6a"
    
    # Create vaultsclient using the default config file (\.oci\config) for auth to the API
    config = oci.config.from_file()
    vaultclient = oci.vault.VaultsClient(config)
    
    # Get the secret
    secretclient = oci.secrets.SecretsClient(config)
    secretcontents = secretclient.get_secret_bundle(secret_id=keyOCID)
    
    # Decode the secret from base64 and print
    keybase64 = secretcontents.data.secret_bundle_content.content
    keybase64bytes = keybase64.encode("ascii")
    keybytes = base64.b64decode(keybase64bytes)
    key = keybytes.decode("ascii")
    print(key)
    
    
    return key
import pandas as pd
import zipfile
from CONFIG import PROXY_FILE_PATH, FOLDER_EXTENSION


def get_df_proxy_by_text_file():
    temp = pd.read_csv(PROXY_FILE_PATH)
    temp.index += 1
    temp.loc[0, temp.columns[0]] = temp.columns[0]
    temp.columns = ["proxy"]
    temp_col = temp["proxy"].apply(lambda x: tuple(x.split(":")))
    temp["host"] = temp_col.str.get(0)
    temp["port"] = temp_col.str.get(1)
    temp["username"] = temp_col.str.get(2)
    temp["password"] = temp_col.str.get(3)
    temp.pop("proxy")
    return temp.sort_index()


def get_manifest_json(id):
    return """
{
    "version": "1.0.0",
    "manifest_version": 2,
    "name": "Proxy_%s",
    "permissions": [
        "proxy",
        "tabs",
        "unlimitedStorage",
        "storage",
        "<all_urls>",
        "webRequest",
        "webRequestBlocking"
    ],
    "background": {
        "scripts": ["background.js"]
    },
    "minimum_chrome_version":"22.0.0"
}
""" % str(id)


def get_background_js(host, port, username, password):
    return """
var config = {
        mode: "fixed_servers",
        rules: {
        singleProxy: {
            scheme: "http",
            host: "%s",
            port: parseInt(%s)
        },
        bypassList: ["localhost"]
        }
    };

chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

function callbackFn(details) {
    return {
        authCredentials: {
            username: "%s",
            password: "%s"
        }
    };
}

chrome.webRequest.onAuthRequired.addListener(
            callbackFn,
            {urls: ["<all_urls>"]},
            ['blocking']
);
""" % (host, port, username, password)


def zip_proxy_extensions():
    df_proxy = get_df_proxy_by_text_file()
    for i in range(len(df_proxy)):
        manifest = get_manifest_json(i)
        host, port, username, password = df_proxy.loc[i]
        background = get_background_js(host, port, username, password)
        file_name = f"{FOLDER_EXTENSION}/proxy_{i}.zip"
        with zipfile.ZipFile(file_name, "w") as zp:
            zp.writestr("manifest.json", manifest)
            zp.writestr("background.js", background)

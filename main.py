from io import BytesIO
import pandas as pd

from flask import Flask, request, send_file
from flask_cors import CORS


app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})


@app.post("/map")
def _map():
    in_data = request.files["input"]
    servidores = request.files["servidores"]
    in_df = pd.read_csv(in_data, sep=";")
    out_df = pd.DataFrame({
        "PROCESSO": [],
        "ZONA ELEITORAL": [],
        "CÓDIGO CLASSE": [],
        "CLASSE JUDICIAL": [],
        "TAREFA/OBSERVAÇÃO": [],
    })
    out_df = out_df.assign(PROCESSO=in_df["nr_processo"])
    in_df["ds_orgao_julgador"].dropna().transform(lambda x: int(x[:3]))
    zonas = in_df["ds_orgao_julgador"].dropna().transform(lambda x: int(x[:3]))
    out_df = out_df.assign(**{"ZONA ELEITORAL": zonas})
    out_df = out_df.assign(**{"TAREFA/OBSERVAÇÃO": in_df["nm_tarefa"]})
    out_df = out_df.assign(**{"CLASSE JUDICIAL": in_df["ds_classe_judicial"]})
    out_df = out_df.assign(**{"CÓDIGO CLASSE": in_df["cd_classe_judicial"]})
    map_df = pd.read_excel(servidores)
    map_df["Servidor"] = map_df["Servidor"].apply(lambda x: x.upper())

    schema = lambda: {
        "PROCESSO": list(),
        "ZONA ELEITORAL": list(),
        "CÓDIGO CLASSE": list(),
        "CLASSE JUDICIAL": list(),
        "TAREFA/OBSERVAÇÃO": list(),
    }
    servidores = {
        servidor: pd.DataFrame(schema()) for servidor in map_df["Servidor"].unique()
    }
    for servidor in servidores:
        zonas = map_df.loc[map_df["Servidor"] == servidor, "ZE"]
        servidores[servidor] = out_df.loc[out_df["ZONA ELEITORAL"].isin(zonas)]
        servidores[servidor]["ZONA ELEITORAL"] = in_df["ds_orgao_julgador"]

    buffer = BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        for sheet_name, df in servidores.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    return send_file(buffer, download_name="output.xlsx", as_attachment=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0")

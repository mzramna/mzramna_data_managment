import json

from mzramna_data_management import MYG_Sheets
from pprint import pprint
gsheet_tool=MYG_Sheets("./quickstart-1551350520829-7f38463817d3.json")
planilha_gerada="1sKDRFosWlk4Moqr4SmTPehsYxYqitWI_dL13NHSUnTM"
planilha_manual="1eXZbyslulb-sodRXQHG7ptJqpt14F8wAB69dDXNDTFw"
newdict=dict({"coluna1":"col1","coluna2":"col2","coluna3":"col3"})

gsheet_tool.delete_page(sheet_id=planilha_gerada,page_number="nova pagina",advanced_debug=True)
print(gsheet_tool.add_page(sheet_id=planilha_gerada,page_name="nova pagina",advanced_debug=True))
newdict=[]
for i in range(0,11):
    newdict.append(dict({"coluna1": "col1 linha "+str(i), "coluna2": "col2 linha "+str(i), "coluna3": "col3 linha "+str(i)}))
gsheet_tool.add_multiple_data_row(sheet_id=planilha_gerada,page_number="nova pagina",elementos=newdict,advanced_debug=True)
pprint(gsheet_tool.retrive_data(planilha_gerada,1,advanced_debug=True))
gsheet_tool.delete_multiple_data_row(sheet_id=planilha_gerada,page_number="nova pagina",row_ids=[3,5,7,9,11],advanced_debug=True)
pprint(gsheet_tool.retrive_data(planilha_gerada,1,advanced_debug=True))
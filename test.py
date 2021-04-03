import csv
import json
import requests


outdata = {'base': "", 'rates': {}, 'last_update': ""} 
for base in base_code:
    url = "https://v6.exchangerate-api.com/v6/ab559bcaff640f3a60259842/latest/"+ base
    res = requests.get(url).text
    res = json.loads(res)
    # res = json.dumps(res)
    res = res.items()
    print(res)
    for 
    outdata = {'base': base, 'rates': {}, 'last_update': res['time_last_update_utc']}


# print(json.dumps(outdata,indent=''))
   
   
def download_rates():
    with open('C:/Users/asaph/OneDrive/Documentos/GitHub/airflow-classes/airflow-materials/airflow-section-3/mnt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for row in reader:
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get('https://v6.exchangerate-api.com/v6/ab559bcaff640f3a60259842/latest/' + base).json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['time_last_update_utc']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['conversion_rates'][pair]
            with open('/usr/local/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')


download_rates()


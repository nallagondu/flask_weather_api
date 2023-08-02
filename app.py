from flask import Flask, request, jsonify
import os
import json
from databricks_api import DatabricksAPI
import requests
# initialising a flask app

application = Flask(__name__)

app = application

def databricks_connection_request(start_index_value,end_index_value):

    token = 'dapi969357e850f42e92050e67fee0a9de73-3'
    db = DatabricksAPI(
        host="https://adb-2857568921143049.9.azuredatabricks.net/",
        token=token
    )
    list_arg=[start_index_value,end_index_value]
    str_list_arg=str(list_arg)

    job_payload = {
        "run_name": 'just_a_run',
        "existing_cluster_id": '0423-212957-vl2qhpwd',
        "notebook_task":
            {
                "notebook_path": '/Shared/MetaDatarepliaction_Backend_Code/Back_End_code',
                "base_parameters": {"list1": str_list_arg}

            }
    }
    resp = requests.post('https://adb-2857568921143049.9.azuredatabricks.net/api/2.0/jobs/runs/submit',
                         json=job_payload, headers={'Authorization': 'Bearer dapi969357e850f42e92050e67fee0a9de73-3'})
    run_id = int(resp.text[10:-1])
    final_result = db.jobs.get_run_output(run_id=run_id)
    print(final_result, job_payload)
    return str(run_id)



@app.route('/', methods = ['GET'])
def home_page():
    return 'Welcome to weather Data'


@app.route('/weather/range', methods=['GET'])
def output_range():
    # df = pd.read_csv(os.path.join('artifacts', 'clean_data.csv'))
    # df_grouped = df.groupby(['city_id', 'year', 'month', 'day', 'hour']).first()
    # starting values
    city_id = request.args.get(key='city_id', type=int)
    year = request.args.get(key='start_year', type=int)
    month = request.args.get(key='start_month', type=int)
    day = request.args.get(key='start_day', type=int)
    hour = request.args.get(key='start_hour', type=int)
    start_index_value ={"city_id":city_id,
                        "year":year,
                        "month":month,
                        "day":day,
                        "hour":hour



    }

    print(start_index_value)

    # ending values
    year = request.args.get(key='end_year', type=int)
    month = request.args.get(key='end_month', type=int)
    day = request.args.get(key='end_day', type=int)
    hour = request.args.get(key='end_hour', type=int)

    end_index_value = {"city_id": city_id,
                         "end_year": year,
                         "end_month": month,
                         "end_day": day,
                         "end_hour": hour

                         }
    a=databricks_connection_request(start_index_value,end_index_value)
    print(a)

    return a


@app.route('/weather', methods = ['GET'])
def get_output():

        
    df=pd.read_csv(os.path.join('artifacts','clean_data.csv'))
    df_grouped = df.groupby(['city_id','year','month','day','hour']).first()
    city_id = request.args.get(key='city_id', type=int)
    year = request.args.get(key='year',type=int)
    month = request.args.get(key='month',type=int)
    day = request.args.get(key='day',type=int)
    hour = request.args.get(key='hour',type=int)
    index_value = (city_id,year,month,day,hour)
    print(index_value)
    print(df_grouped.loc[index_value, :])
    print(df_grouped.loc[index_value, :].to_dict())
    return df_grouped.loc[index_value, :].to_dict()




    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port = 5000)


        
       


        

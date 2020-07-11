import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
import psycopg2
import json
import plotly.graph_objects as go
import datetime
import random

with open('./config.json') as cf:
    config = json.load(cf)

usage_data = pd.DataFrame(columns=['industrial','public','residential'])
total_generation = list()

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__,external_stylesheets = external_stylesheets)


# CSS
colors = {
    'background':'#FFFFFF', #'#D2D2D2',
    'graph_background': '#FFFFFF',
    'text': '#4A494B',
    'line_color': ['rgb(173,141,219)','rgb(152,113,211)','rgb(112, 56, 193)']
}

mini_container = {
  "border-radius": "3px",
  "background-color": "#f9f9f9",
  "margin": "5px",
  "padding": "15px",
  "position": "relative",
  "box-shadow": "2px 2px 2px lightgrey"
}

# connect to DB
mytimezone = 'America/Los_Angeles'
try:
    connection = psycopg2.connect(**config['postgres_conn'])
    cursor = connection.cursor()
    cursor.execute('SHOW TIMEZONE;')     # check time zone
    db_timezone = cursor.fetchone()[0]
    print(db_timezone)
    if db_timezone!=mytimezone:
        cursor.execute("""SET TIMEZONE='{}';""".format(mytimezone))
    cursor.execute('SHOW TIMEZONE;')     # check time zone
    db_second_timezone = cursor.fetchone()[0]
    print(db_second_timezone)
except:
    print("can't connect to the database.")



app.layout = html.Div(style={'backgroundColor': colors['background']}, 
    children=[
    html.H1(children='Real time electric load monitor',
            style={
            'textAlign': 'center',
            'color': colors['text']
        }),
    html.Div(className='row flex-display',children=[
        html.Div(className='pretty_container eight columns',children=[
        html.Div(className = 'temp', children=[
            html.Div(className='row container-display',id='info',children=[
                html.Div([html.H6(id='running_num_text'),html.P('No. of Running Machines')],className='mini_container',id='running_num',style = mini_container),
                html.Div([html.H6(id='machines_num_text'), html.P('No. of Machines')],className='mini_container',id='machines_num',style = mini_container),
                html.Div([html.H6(id='households_num_text'),html.P('No. of Households')],className='mini_container',id='households_num',style = mini_container),
            ],style = {"display": "flex","align-items": "center"}),
            dcc.Graph(id='live-usage-monitor', animate=True)])]),
        html.Div(className='pretty_container four columns',id='live-anomalies'),
    ]),
    dcc.Interval(
            id='interval-component',
            interval=5*1000,  # 5sec
            n_intervals=0)

])


@app.callback(
                [Output('machines_num_text', 'children'),
                Output('households_num_text', 'children'),
                Output('running_num_text', 'children'),], [Input('interval-component', 'n_intervals')])
def update_info(n):
    query_no_machines = """SELECT COUNT(machine_id) FROM machines; """
    query_no_households = """SELECT COUNT(household_id) FROM households; """
    query_running_machines = """
                SELECT COUNT(machine_id) FROM events
                WHERE timestamp = ( SELECT MAX(timestamp) FROM events) AND usage>0;
                """
    cursor.execute(query_no_machines)
    m = cursor.fetchone()
    cursor.execute(query_no_households)
    h =cursor.fetchone()
    cursor.execute(query_running_machines)
    r = cursor.fetchone()
    return m[0],h[0],r[0]

@app.callback(Output('live-anomalies', 'children'), [Input('interval-component', 'n_intervals')])
def update_news(n):
    query = """
        SELECT machine_id, machine_type, machines.household_id, household_address
        FROM machines, households 
        WHERE machine_id IN (SELECT DISTINCT machine_id FROM anomalies WHERE timestamp > NOW() - INTERVAL '30 MIN')
        AND machine_type !='grid_import'
        AND machines.household_id = households.household_id;
    """
    df = pd.read_sql(query,connection)
    anormalies = json.loads(df.to_json(orient='records'))
    children_struct = []
    title = html.H5("Anomalies List",style={"margin-top": "20px"})
    children_struct.append(title)
    for i, anormaly in enumerate(anormalies):
        one_block = html.Div(id='anormaly_'+str(i), className="row details",
                        children = [
                            html.P('Machine ID: ' + anormaly['machine_id']),
                            html.P('Machine Type: ' + anormaly['machine_type']),
                            html.P('Household ID: '+ anormaly['household_id']),
                            html.P('Address: '+ anormaly['household_address']),
                        ],style = mini_container)
        children_struct.append(one_block)

    return html.Div(id="Anomalies_List", className="row container-display", style={'margin-left':-50,'margin-right':0},
        children = children_struct)

@app.callback(Output('live-usage-monitor', 'figure'), [Input('interval-component', 'n_intervals')])
def update_graph(n):
    try:
        global usage_data
        global total_generation
        query = """
                SELECT household_type,timestamp, SUM(usage) FROM events,households
                WHERE timestamp = ( SELECT MAX(timestamp) FROM events) and events.household_id = households.household_id  
                GROUP BY household_type, timestamp ;
                """
        df = pd.read_sql(query,connection)
        df_new = df.pivot(index='timestamp',columns='household_type',values='sum')
        usage_data=usage_data.append(df_new)
        if usage_data.shape[0]<=5:
            total_generation.append(usage_data.agg(sum).sum()*random.uniform(1.2,1.5))
        else:
            total_generation.append(usage_data.agg(sum).mean()*random.uniform(0.8,1.2))
        title=go.layout.Title(text="Usage")
        fig = go.Figure()
        line_color = ['rgb(173,141,219)','rgb(152,113,211)','rgb(112, 56, 193)'] # shallow to dark
        fig.add_trace(go.Scatter(name = 'Industrial',x=usage_data.index, y=usage_data['industrial'],mode= 'lines',line_color=colors['line_color'][2], fill='tozeroy')) # fill down to xaxis
        fig.add_trace(go.Scatter(name = 'Residential',x=usage_data.index, y=usage_data['industrial']+ usage_data['residential'],mode= 'lines',line_color=colors['line_color'][1], fill='tonexty'))
        fig.add_trace(go.Scatter(name = 'Public',x=usage_data.index, y=usage_data['industrial']+ usage_data['residential']+usage_data['public'],mode= 'lines', line_color=colors['line_color'][0], fill='tonexty'))
        fig.add_trace(go.Scatter(name = 'Total Generated',x=usage_data.index, y=total_generation, mode='lines', line_color='rgb(199,0,57)'))
        fig.update_layout(legend_orientation="h",legend=dict(x=-.1, y=-0.2))
        fig.update_layout(width=850, height=450, title_text='Total Usage', plot_bgcolor=colors['graph_background'])
        fig.update_layout(xaxis_title = 'time', yaxis_title='wh')
        fig.update_layout(xaxis = dict(range=[min(usage_data.index),max(usage_data.index)]))
                        #yaxis = dict(range=[0,max(total_generation)]))

        return fig
    except Exception as e:
        print("Couldn't update bar-graph")
        print(e)


if __name__ == '__main__':
    app.run_server(debug=True)

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
import psycopg2
import json
import plotly.graph_objects as go
import datetime
from collections import deque
import random
# from pyorbital.orbital import Orbital
# satellite = Orbital('TERRA')



'''
TO-Do
abnormal chart (limit 5 for the past 30 min)
Add number of running machine
total machines, 

https://www.youtube.com/watch?v=luixWRpp6Jo
https://github.com/plotly/dash-sample-apps/blob/master/apps/dash-oil-and-gas/app.py
https://dash-gallery.plotly.host/dash-oil-and-gas/

'''


with open('./config.json') as cf:
    config = json.load(cf)

mytimezone = 'America/Los_Angeles'

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__,external_stylesheets = external_stylesheets)

colors = {
    'background': '#D2D2D2',
    'graph_background': '#FFFFFF',
    'text': '#4A494B',
    'line_color': ['rgb(173,141,219)','rgb(152,113,211)','rgb(112, 56, 193)']
}

usage_data = pd.DataFrame(columns=['industrial','public','residential'])
anormalies = 0
try:
    connection = psycopg2.connect(**config['postgres_conn'])
    cursor = connection.cursor()
    # check time zone
    cursor.execute('SHOW TIMEZONE;')
    if cursor.fetchone()[0]!=mytimezone:
        cursor.execute("""SET TIMEZONE='{}';""".format(mytimezone))
except:
    print("can't connect to the database.")

app.layout = html.Div(style={'backgroundColor': colors['background']}, 
    children=[
    html.H1(children='Real time electric load monitor',
            style={
            'textAlign': 'center',
            'color': colors['text']
        }),

    html.Div(children='''
        Dash: A web application framework for Python.
    ''', style={
        'textAlign': 'center',
        'color': colors['text']
    }),
    # html.Div(className='anormalies_form',children = [html.Div(id='anormalies',children=update_news())]),
    html.Div(className='row flex-display',children=[
        html.Div(className='pretty_container eight columns',children=[dcc.Graph(id='live-usage-monitor', animate=True)]),
        html.Div(className='pretty_container four columns',id='live-anormalies'),
    ]),
    dcc.Interval(
            id='interval-component',
            interval=5*1000,  # 5sec
            n_intervals=0)

])

@app.callback(Output('live-anormalies', 'children'), [Input('interval-component', 'n_intervals')])
def update_news(n):
    query = """
        SELECT * FROM anomalies,machines,households
        WHERE anomalies.machine_id =machines.machine_id AND anomalies.household_id = households.household_id
        AND timestamp = NOW() - INTERVAL '30 MIN'
    """

    return html.Div(id="Anomalies List", className="row details",
        children = [ html.P('Machine ID:',html.H6(id=''), # className? 
                     html.P('Machine Type:',className='machine_id'),
                     html.P('Household ID:',className='machine_id'),
                     html.P('Address:',className='machine_id'),
                     html.P('Machine Type',className='machine_id'),]
    )

@app.callback(Output('live-usage-monitor', 'figure'), [Input('interval-component', 'n_intervals')])
def update_graph(n):
    try:
        global usage_data
        # select the last event
        # query = """
        #         SELECT household_type,timestamp, SUM(usage) FROM events,households
        #         WHERE timestamp = ( SELECT MAX(timestamp) FROM events) and events.household_id = households.household_id  
        #         GROUP BY household_type, timestamp ;
        #         """
        # df = pd.read_sql(query,connection)
        # df_new = df.pivot(index='timestamp',columns='household_type',values='sum')
        # usage_data=usage_data.append(df_new)
        df_new = pd.DataFrame({'industrial':[random.uniform(1,10)],'residential':[random.uniform(1,10)],'public':[random.uniform(1,10)]})
        usage_data=usage_data.append(df_new,ignore_index=True)
        print(usage_data)
        title=go.layout.Title(text="Usage")
        fig = go.Figure()
        line_color = ['rgb(173,141,219)','rgb(152,113,211)','rgb(112, 56, 193)'] # shallow to dark
        fig.add_trace(go.Scatter(name = 'Industrial',x=usage_data.index, y=usage_data['industrial'],mode= 'lines',line_color=colors['line_color'][2], fill='tozeroy')) # fill down to xaxis
        fig.add_trace(go.Scatter(name = 'Residential',x=usage_data.index, y=usage_data['industrial']+ usage_data['residential'],mode= 'lines',line_color=colors['line_color'][1], fill='tonexty'))
        fig.add_trace(go.Scatter(name = 'Public',x=usage_data.index, y=usage_data['industrial']+ usage_data['residential']+usage_data['public'],mode= 'lines', line_color=colors['line_color'][0], fill='tonexty'))
        fig.add_trace(go.Scatter(name = 'Total Generated',x=usage_data.index, y=(usage_data['industrial']+ usage_data['residential']+usage_data['public'])*random.uniform(1.15,1.5),mode= 'lines', line_color='rgb(199,0,57)'))
        fig.update_layout(legend_orientation="h",legend=dict(x=-.1, y=-0.2))
        fig.update_layout(width=850, height=450, title_text='Total Usage', plot_bgcolor=colors['graph_background'])
        fig.update_layout(xaxis_title = 'time', yaxis_title='wh')

        return fig
    except Exception as e:
        print("Couldn't update bar-graph")
        print(e)







if __name__ == '__main__':
    app.run_server(debug=True)



'''
# 
query = """
        SELECT household_type, SUM(usage) FROM history_events, households
        WHERE history_events.household_id = households.household_id  
        AND timestamp >= TO_TIMESTAMP('2018-10-30 02:53:00','YYYY-MM-DD HH:MI:SS') - INTERVAL '5 MIN'
        GROUP BY household_type;
        """

query = """
            SELECT machines.machine_id, machine_type FROM history_events,households,machines
            WHERE households.household_id = history_events.household_id and machines.machine_id=history_events.machine_id and timestamp >= TO_TIMESTAMP('2018-10-30 02:53:00','YYYY-MM-DD HH:MI:SS') - INTERVAL '5 MIN' and abnormal=True
            GROUP BY machines.machine_id;
            """
'''
import pandas as pd
import plotly.graph_objects as go
import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output


airline_data =  pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-DV0101EN-SkillsNetwork/Data%20Files/airline_data.csv', 
                            encoding = "ISO-8859-1",
                            dtype={'Div1Airport': str, 'Div1TailNum': str, 
                                   'Div2Airport': str, 'Div2TailNum': str})

                                   # Create a dash application
app = dash.Dash(__name__)
app.layout = html.Div(children=[html.H1('Airline Performance Dashboard',style={'textAlign': 'center', 'color': '#503D36', 'font-size': 40}),
                                html.Div(["Input Year", dcc.Input(),], 
                                style={}),
                                html.Br(),
                                html.Br(),
                                html.Div(),
                                ])
# Run the app
if __name__ == '__main__':
    app.run_server()

    

#  Terminal
#  python3 dash_interactivity.py
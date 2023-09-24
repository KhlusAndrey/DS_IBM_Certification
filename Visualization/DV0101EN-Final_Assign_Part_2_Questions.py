#!/usr/bin/env python
# coding: utf-8

# In[ ]:

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.graph_objs as go
import plotly.express as px

# Load the data using pandas
data = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-DV0101EN-SkillsNetwork/Data%20Files/historical_automobile_sales.csv')

# Initialize the Dash app
app = dash.Dash(__name__)

# Set the title of the dashboard
app.title = "Automobile Statistics Dashboard"

year_list = [i for i in range(1980, 2024, 1)]
# Create the layout of the app
app.layout = html.Div([
    html.H1("Automobile Sales Statistics Dashboard", 
        style={'textAlign': 'left', 'color': '#503D36', 'font-size': 24}),
    html.Div([
        html.Label("Select Statistics:"),
        dcc.Dropdown(
            id='dropdown-statistics',
            options=[
                {'label': 'Yearly Statistics', 'value': 'Yearly Statistics'},
                {'label': 'Recession Period Statistics', 'value': 'Recession Period Statistics'}
            ],
            value='Select Statistics',
            placeholder='Select a report type.',
            style={'textAlign': 'center', 'width': '80%', 'padding': '3px', 'font-size': 20}
        ),
        dcc.Dropdown(
            id='select-year',
            options=[{'label': i, 'value': i} for i in year_list],
            value='1980'
        ),
    ]),
    html.Div(id='output-container', className='chart-grid', style={'display': 'flex'}),
])

# Callbacks
@app.callback(
    Output(component_id='select-year', component_property='disabled'),
    Input(component_id='dropdown-statistics', component_property='value')
)
def update_input_container(selected_statistics):
    if selected_statistics == 'Yearly Statistics':
        return False
    else:
        return True

@app.callback(
    Output(component_id='output-container', component_property='children'),
    [Input(component_id='select-year', component_property='value'), 
     Input(component_id='dropdown-statistics', component_property='value')]
)


def update_output_container(input_year, selected_statistics):
    if selected_statistics == 'Recession Period Statistics':
        # Filter the data for recession periods
        recession_data = data[data['Recession'] == 1]
        
        # TASK 2.5: Create and display graphs for Recession Report Statistics
        
        # Plot 1: Automobile sales fluctuate over Recession Period (year-wise)
        yearly_rec = recession_data.groupby('Year')['Automobile_Sales'].mean().reset_index()
        R_chart1 = dcc.Graph(
            figure=px.line(yearly_rec, x='Year', y='Automobile_Sales',
                            title="Average Automobile Sales Fluctuation over Recession Period")
        )

        # Plot 2: Calculate the average number of vehicles sold by vehicle type
        average_sales = recession_data.groupby('Vehicle_Type')['Automobile_Sales'].mean().reset_index()
        R_chart2 = dcc.Graph(
            figure=px.bar(average_sales, x='Vehicle_Type', y='Automobile_Sales',
                           title="Average Number of Vehicles Sold by Vehicle Type during Recessions")
        )

        # Plot 3: Pie chart for total expenditure share by vehicle type during recessions
        exp_rec = recession_data.groupby('Vehicle_Type')['Advertising_Expenditure'].sum().reset_index()
        R_chart3 = dcc.Graph(
            figure=px.pie(exp_rec, values='Advertising_Expenditure', names='Advertising_Expenditure',
                           title="Total Expenditure Share by Vehicle Type during Recessions")
        )

        # Plot 4: Bar chart for the effect of the unemployment rate on vehicle type and sales
        unemployment_sales= recession_data.groupby(['Vehicle_Type', 'unemployment_rate'])['Automobile_Sales'].mean().reset_index()
        R_chart4 = dcc.Graph(
            figure=px.bar(unemployment_sales, x='unemployment_rate', y='Automobile_Sales', 
            color='Vehicle_Type', title='The effect of unemployment rate on vehicle type and sales')
        )
        return [
            html.Div(className='chart-item', children=[html.Div(children=R_chart1), html.Div(children=R_chart2)],
                     style={'display': 'flex'}),
            html.Div(className='chart-item', children=[html.Div(children=R_chart3), html.Div(children=R_chart4)],
                     style={'display': 'flex'})
        ]

    # TASK 2.6: Create and display graphs for Yearly Report Statistics
    elif input_year and selected_statistics == 'Yearly Statistics':
        yearly_data = data[data['Year'] == input_year]

        # TASK 2.5: Creating Graphs for Yearly Data

        # Plot 1: Yearly Automobile sales using a line chart for the whole period
        yearly_rec=yearly_data.groupby('Year')['Automobile_Sales'].mean().reset_index()
        yas= data.groupby('Year')['Automobile_Sales'].mean().reset_index()
        Y_chart1 = dcc.Graph(figure=px.line(yas, x='Year', y='Automobile_Sales', title ='Yearly Automobile sales'))
            
        # Plot 2: Total Monthly Automobile sales using a line chart
        Y_chart2 = dcc.Graph(
            figure=px.line(yearly_data, x='Month', y='Automobile_Sales',
                            title="Total Monthly Automobile Sales")
        )

        # Plot 3: Bar chart for the average number of vehicles sold during the given year
        avr_vdata = yearly_data.groupby('Vehicle_Type')['Automobile_Sales'].mean().reset_index()
        Y_chart3 = dcc.Graph(
            figure=px.bar(avr_vdata, x='Vehicle_Type', y='Automobile_Sales',
                           title='Average Vehicles Sold by Vehicle Type in the year {}'.format(input_year))
        )

        # Plot 4: Pie chart for Total Advertisement Expenditure for each vehicle
        exp_data = yearly_data.groupby('Vehicle_Type')['Advertising_Expenditure'].sum().reset_index()
        Y_chart4 = dcc.Graph(
            figure=px.pie(exp_data, values='Advertising_Expenditure', names='Advertising_Expenditure',
                           title="Total Advertisement Expenditure by Vehicle Type")
        )

        # TASK 2.6: Returning the graphs for displaying Yearly data
        return [
            html.Div(className='chart-item', children=[html.Div(children=Y_chart1), html.Div(children=Y_chart2)],
                     style={'display': 'flex'}),
            html.Div(className='chart-item', children=[html.Div(children=Y_chart3), html.Div(children=Y_chart4)],
                     style={'display': 'flex'})
        ]
    else:
        return None


# Run the Dash app
if __name__ == '__main__':
    app.run_server(debug=True)



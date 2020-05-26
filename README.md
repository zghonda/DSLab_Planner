# Robust Journey Planning 

A robust planner for the city of Zurich that comes as a solution for the multi-modal problem described by the following: 

Given a desired arrival time, an arrival and departure locations what will be the fastest and less risky multi-modal 
route that takes the user to its destination. 

For that, we use GTFS open data available in [Open data platform mobility Switzerland](https://opentransportdata.swiss/en/) 
for finding the routes supported 
by a custom delay prediction model injesting around 2 years of SBB [actual data](https://opentransportdata.swiss/de/dataset/istdaten)

## Technical approach 

We use A* star search algorithm to find the solution of the problem. All parts where designed around the planner and its
requirements. Please see `robust_planner` notebook.

## Content 

- Data Analysis and Exploration 
- Public transport network model
- Predictive model
- Data format transformation and indexation
- Route planning Algorithm
- Validation
- Visualization

## Files descriptions

### `main_notebook ` 

Main data analysis notebook in which we heavily filter SBB and GTFS data, combine and aggregate it to model the zurich public
transport network and find insights on which our predictive model will based on. 

### `data_preparation`

Transformation and indexation of the data generated in `main_notebook` into lookup tables in a pythonic way to smooth and
facilitate the implementation of the multi modal planner

### `robust_planner`

Description of the algorithmic approach used and design choices, see implementation in `scripts/robust_planner.py`

### `validation`

Validation part for the planner using SBB real data 


### `dashboard`

Web visualization tool used to show the output of the planner


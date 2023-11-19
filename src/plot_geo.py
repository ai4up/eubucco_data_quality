import geopandas as gpd
from geopandas.geodataframe import GeoDataFrame as gdf
from typing import Optional
import contextily as cx
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os

MAP_BOX_KEY = os.getenv('MAP_BOX_KEY')

def plot_radius(df: gdf, target: gdf, rad: int=300, 
                feature: Optional[str]=None, 
                basemap: bool=False,
                ax=None,
                legend=False):
    
    targ_cent = target.geometry.centroid
    
    circle =  targ_cent.buffer(rad)
    idx = df.sindex.query(circle, predicate="intersects")
    idx = idx[1,:]
    df_plot = df.iloc[idx].copy()
    
    if basemap:
        alpha=0.8
    else:
        alpha=1
        
        
    ax = target.plot(ax=ax, marker='x', color="blue", zorder=10)
    ax = df_plot.plot(ax=ax, figsize=(8,6), column=feature, legend=legend, 
                        legend_kwds={"label": feature}, alpha=alpha, zorder=5,
                        vmin=0, vmax=10
                        )
    
    ax.set_axis_off()

    if basemap:
        cx.add_basemap(ax, crs=df_plot.crs.to_string(),
                       source="https://api.mapbox.com/styles/v1/mapbox/streets-v11/tiles/{z}/{x}/{y}{r}?access_token={MAP_BOX_KEY}", 
                       zoom=18, zorder=1)
        
    return ax

def compiled_plot(df: gdf, target: gdf, rad: int=250, 
                feature: Optional[str]=None):

    # using tuple unpacking for multiple Axes
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16,7))
    
    plot_radius(ax=ax1, df=df, target=target, feature=feature, legend=True)
    plot_radius(ax=ax2, df=df, target=target, feature=feature, basemap=True, rad=70, legend=False)
    print(target.centroid.to_crs("EPSG:4326"))

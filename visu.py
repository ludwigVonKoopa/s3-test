from matplotlib import pyplot as plt

import cartopy.crs as ccrs
import cartopy.feature as cfeature


def start_ax():
    fig, ax = plt.subplots(1, 1, figsize=(12, 7), dpi=120,
        subplot_kw={"projection": ccrs.PlateCarree()}
    )

    ax.coastlines()
    ax.add_feature(cfeature.LAND)

    gridlines = ax.gridlines(alpha=0.2, color="black",
        draw_labels=True, dms=True, linestyle=":",
    )

    gridlines.right_labels = False
    gridlines.top_labels  = False
    return fig, ax

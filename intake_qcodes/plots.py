from intake_qcodes.datasets import parameters_from_description

def _make_axis_label(spec):
    _name = spec['name']
    _label = spec['label']

    if _label:
        label = _label
    else:
        label = _name

    unit = spec['unit']
    if unit:
        plot_label = f'{label} ({unit})'
    else:
        plot_label = f'{label}'

    return plot_label

def make_default_plots(run_description):
    dep_params, _ = parameters_from_description(run_description)
    specs = run_description['interdependencies']['paramspecs']

    plots = {}
    for param in dep_params:
        pspec = next(d for d in specs if d['name']==param)
        if len(pspec['depends_on'])==1:

            y = param
            ylabel = _make_axis_label(pspec)
            x = pspec['depends_on'][0]
            xspec = next(d for d in specs if d['name']==x)
            xlabel = _make_axis_label(xspec)

            plots[param] = {
                'kind': 'scatter',
                'x': x,
                'xlabel': xlabel,
                'y': y,
                'ylabel': ylabel,
                'width': 600,
                'height': 400,
                'padding': 0.01,
                'xformatter': "%.1e",
                'yformatter': "%.3e",
            }

        elif len(pspec['depends_on'])==2:
            z = param
            zlabel = _make_axis_label(pspec)
            x = pspec['depends_on'][0]
            xspec = next(d for d in specs if d['name']==x)
            xlabel = _make_axis_label(xspec)
            y = pspec['depends_on'][1]
            yspec = next(d for d in specs if d['name']==y)
            ylabel = _make_axis_label(xspec)

            plots[param] = {
                'kind': 'quadmesh',
                'x': x,
                'xlabel': xlabel,
                'y': y,
                'ylabel': ylabel,
                'z': z,
                'clabel': zlabel,
                'cmap': 'Plasma',
                'colorbar': True,
                'width': 600,
                'height': 400,
                'padding': 0.005,
                'xformatter': "%.1e",
                'yformatter': "%.1e",
            }
            pass
        else:
            continue
    return plots

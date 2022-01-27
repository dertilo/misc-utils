from dataclasses import dataclass
from typing import Any, Optional

import dash_core_components as dcc
import dash_html_components as html
from beartype import beartype
from dash import Dash
from dash.dependencies import Input, Output, State

from misc_utils.beartypes import bear_does_roar
from misc_utils.dataclass_utils import serialize_dataclass, deserialize_dataclass, \
    encode_dataclass
from misc_utils.utils import just_try
from plotly_dash_utils.plotly_dash_dataclassed import DashDataclasses

app = DashDataclasses(__name__)

app.layout = html.Div(
    [
        html.H6("Change the value in the text box to see callbacks in action!"),
        html.Div(
            ["Input: ", dcc.Input(id="my-input", value="initial value", type="text")]
        ),
        html.Br(),
        html.Div(id="my-output"),
        dcc.Store("my-state"),
    ]
)

assert bear_does_roar(lambda : encode_dataclass(None))

@dataclass
class SimpleDataClass:
    count: int = 0



@app.callback_dataclassed(
    Output(component_id="my-output", component_property="children"),
    Output("my-state", "data"),
    Input(component_id="my-input", component_property="value"),
    State("my-state", "data"),
)
def update_output_div(
    input_value, state: Optional[SimpleDataClass]
) -> tuple[dict, SimpleDataClass]:
    print(f"{type(state)=}")
    if state is not None:
        count = state.count
    else:
        count = 0
    return {"dict": input_value, "state": state}, SimpleDataClass(count=count + 1)


if __name__ == "__main__":
    app.run_server(debug=True)
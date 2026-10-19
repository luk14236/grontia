"""NDW DATEX II parsing shared by the daily (30) and live (60) traffic DAGs."""
from __future__ import annotations

import io
import xml.etree.ElementTree as ET


def ndw_xml_to_records(xml_bytes: bytes) -> list[dict]:
    """
    Parse an NDW DATEX II MeasuredDataPublication (e.g. trafficspeed.xml.gz)
    into one record per measurement site.

    Each <siteMeasurements> holds several <measuredValue> entries per lane /
    vehicle class: TrafficFlow values carry vehicleFlowRate (vehicles/hour) and
    TrafficSpeed values carry averageVehicleSpeed/speed (km/h). Negative values
    mean "no data". We sum the flows and average the valid speeds per site.
    """
    def local(tag: str) -> str:
        return tag.rsplit("}", 1)[-1]

    records = []
    for _, elem in ET.iterparse(io.BytesIO(xml_bytes), events=("end",)):
        if local(elem.tag) != "siteMeasurements":
            continue

        site_id, measured_at = None, None
        flows: list[float] = []
        speeds: list[float] = []
        for child in elem.iter():
            name = local(child.tag)
            if name == "measurementSiteReference":
                site_id = child.get("id")
            elif name == "measurementTimeDefault":
                measured_at = child.text
            elif name == "vehicleFlowRate" and child.text:
                if float(child.text) >= 0:
                    flows.append(float(child.text))
            elif name == "speed" and child.text:
                if float(child.text) > 0:
                    speeds.append(float(child.text))

        records.append({
            "id": site_id,
            "element_type": "siteMeasurements",
            "measurementSiteReference": site_id,
            "period": measured_at,
            "vehicleFlowRate": sum(flows) if flows else None,
            "averageVehicleSpeed": round(sum(speeds) / len(speeds), 1) if speeds else None,
        })
        elem.clear()

    return records

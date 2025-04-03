"use client";

import { VChart } from "@visactor/react-vchart";
import type { ICirclePackingChartSpec } from "@visactor/vchart";
import convertions from "../../../../app-data/convertionOcupation";
import { addThousandsSeparator } from "../../utils";

const chartData = [
  {
    name: "Occupations",
    children: convertions.map((item) => ({
      name: item.occupation, // Occupation name
      value: item.employment, // Employment value
    })),
  },
];

console.log(chartData);

const spec: ICirclePackingChartSpec = {
  data: [
    {
      id: "data",
      values: chartData,
    },
  ],
  type: "circlePacking",
  categoryField: "name",
  valueField: "value",
  drill: true,
  padding: 0,
  layoutPadding: 5,
  label: {
    style: {
      fill: "white",
      stroke: false,
      visible: (d) => d.depth > 0,
      text: (d) => addThousandsSeparator(d.value),
      fontSize: (d) => d.radius / 2,
      dy: (d) => d.radius / 8,
    },
  },
  legends: {
    visible: true,
    orient: "top",
    position: "start",
    padding: 0,
    field: "name", // Use the "name" field for legend labels
  },
  /*
  tooltip: {
    trigger: ["click", "hover"],
    mark: {
      content: {
        value: (d) => {
          console.log(d); // Logs the data to the browser console for debugging
          return addThousandsSeparator(d?.value); // Returns the formatted value to display in the tooltip
        },
      },
    },
  },*/
  tooltip: {
    trigger: ["click", "hover"],
    mark: {
      content: {
        // title: (d) => d.name,
        value: (d) => `${d?.name ?? "Unknown"}: ${addThousandsSeparator(d?.value ?? 0)}`,
      },
    },
  },
  
  animationEnter: { easing: "cubicInOut" },
  animationExit: { easing: "cubicInOut" },
  animationUpdate: { easing: "cubicInOut" },
};

export default function Chart() {
  return <VChart spec={spec} />;
}
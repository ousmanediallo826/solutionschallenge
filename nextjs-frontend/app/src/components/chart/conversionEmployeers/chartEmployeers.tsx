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

console.log("Chart Data:", chartData);

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
      text: (d) => d.value,//addThousandsSeparator(d.value)
      fontSize: (d) => d.radius / 2,
      dy: (d) => d.radius / 8,
      
    },
  },
  tooltip: {
    trigger: ["click", "hover"],
    mark: {
      content: {
        value: (d) => {
          console.log("Tooltip Data:", d); // Debugging
          return d?.value ?? "No data"; // Fallback if `d.value` is undefined
        },
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
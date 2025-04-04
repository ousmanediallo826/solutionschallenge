"use client";

import { VChart } from "@visactor/react-vchart";
import type { ICirclePackingChartSpec } from "@visactor/vchart";
import convertions from "@/app/app-data/convertionOcupation";

// 🎨 Extended vivid color palette
const colors = [
  "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
  "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
  "#f94144", "#f3722c", "#f8961e", "#f9c74f", "#90be6d",
  "#43aa8b", "#577590", "#277da1", "#4d908e", "#ff595e",
  "#1982c4", "#6a4c93", "#ffca3a", "#8ac926"
];

// 🔢 Deterministic hash-to-colorIndex function
function getColorIndex(name: string): number {
  let hash = 0;
  for (let i = 0; i < name.length; i++) {
    hash = name.charCodeAt(i) + ((hash << 5) - hash);
  }
  return Math.abs(hash) % colors.length;
}

// 🧠 Format data with colorIndex and a root group
const chartData = [
  {
    name: "Occupations",
    children: convertions.map((item) => ({
      name: item.occupation,
      value: item.employment,
      colorIndex: getColorIndex(item.occupation),
    })),
  }
];

console.log("Chart Data:", chartData); // Debugging

// 📊 VChart spec
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
  drill: false, // ❌ Disable zoom/click

  circlePacking: {
    style: {
      fill: (d) => {
        const idx = d?.datum?.colorIndex;
        return d.isLeaf && typeof idx === "number" ? colors[idx] : "#ccc";
      },
      fillOpacity: (d) => (d.isLeaf ? 0.85 : 0.25),
      cursor: () => "default",
    },
  },

  layoutPadding: [0, 10, 10],

  label: {
    style: {
      fontSize: 10,
      visible: (d) => d.depth === 1,
    },
  },

  tooltip: {
    trigger: ["click", "hover"],
    mark: {
      content: {
        value: (d) => {
          const name = d?.datum?.name ?? "Unknown";
          const value = d?.value?.toLocaleString?.() ?? "N/A";
          return `${name}<br/>Employment: ${value}`;
        },
      },
    },
  },

  title: {
    visible: true,
    text: "Occupational Employment",
  },

  animationEnter: { easing: "cubicInOut" },
  animationExit: { easing: "cubicInOut" },
  animationUpdate: { easing: "cubicInOut" },
};

export default function ChartEmployeeers() {
  return (
    <div style={{ width: "100%", height: "600px" }}>
      <VChart spec={spec} />
    </div>
  );
}

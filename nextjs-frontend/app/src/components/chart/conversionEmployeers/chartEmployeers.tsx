"use client";

import { VChart } from "@visactor/react-vchart";
import type { ICirclePackingChartSpec } from "@visactor/vchart";
import convertions from "@/app/app-data/convertionOcupation";

// 🎨 Color Palette
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

// 📊 Chart Data
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

// 🛠️ Chart Specification
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
  drill: false,

  circlePacking: {
    style: {
      fill: (d) => {
        const idx = d?.datum?.colorIndex;
        return d.isLeaf && typeof idx === "number" ? colors[idx] : "#ccc";
      },
      fillOpacity: (d) => (d.isLeaf ? 0.9 : 0.3), // Slightly increased opacity
      cursor: () => "default",
      stroke: "#555", // Added a dark gray border
      lineWidth: 0.75,
    },
  },

  layoutPadding: [10, 10, 10], // Slightly increased padding

  label: {
    style: {
      fontSize: 12, // Slightly larger label
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
          return [
            { text: `${name}`, fontWeight: "bold" },
            { text: `\nEmployment: ${value}` },
          ];
        },
      },
    },
  },

  title: {
    visible: true,
    text: "Occupational Employment",
    style: {
      fontSize: 18, // Larger title
    },
  },

  animationEnter: { easing: "cubicInOut" },
  animationExit: { easing: "cubicInOut" },
  animationUpdate: { easing: "cubicInOut" },
};

// 🖼️ Chart Component
export default function ChartEmployeeers() {
  return (
    <div style={{ width: "400px", height: "300px" }}> {/* Increased container size */}
      <VChart spec={spec} />
    </div>
  );
}
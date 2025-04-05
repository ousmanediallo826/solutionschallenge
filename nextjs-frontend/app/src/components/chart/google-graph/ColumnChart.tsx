'use client';

import React from "react";
import { Chart } from "react-google-charts";
import convertions from "../../../../app-data/convertionOcupation"; // Import the data

// 🎨 Predefined Color Palette
const colors = [
  "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
  "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
];

// 📊 Chart Data
const data = [
  ["Occupation", "Employment", { role: "style" }], // Header row
  ...convertions.map((item, index) => [
    item.occupation, // Occupation name
    item.employment, // Employment value
    `color: ${colors[index % colors.length]}`, // Predefined color for each bar
  ]),
];

// 🛠️ Chart Options
const options = {
  title: 'Occupational Employment',
  legend: { position: 'top' },
  bar: { groupWidth: '80%' },
  vAxis: {
    title: 'Employment',
    titleTextStyle: { fontSize: 18 },
    textStyle: { fontSize: 14 },
  },
  hAxis: {
    title: 'Occupation',
    titleTextStyle: { fontSize: 18 },
    textStyle: { fontSize: 12 },
    slantedText: true,
    slantedTextAngle: 30,
  },
  titleTextStyle: { fontSize: 24 },
  tooltip: { isHtml: true, textStyle: { fontSize: 14 } },
};

// 🖼️ Chart Component
export function AppColumn() {
  return (
    <div className="w-full h-auto max-w-4xl max-h-[700px]">
      <Chart
        chartType="ColumnChart"
        width="100%"
        height="100%"
        data={data}
        options={options}
      />
    </div>
  );
}
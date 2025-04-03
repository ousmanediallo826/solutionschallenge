/*import React, { useRef, useEffect } from "react";
import * as d3 from "d3";

interface PackedBubbleChartProps {
  data: { name: string; value: number }[];
}

function PackedBubbleChart({ data }: PackedBubbleChartProps) {
  const chartRef = useRef<SVGSVGElement | null>(null);

  useEffect(() => {
    const svg = d3.select(chartRef.current);
    const width = chartRef.current?.clientWidth || 600;
    const height = chartRef.current?.clientHeight || 400;

    // Clear previous content
    svg.selectAll("*").remove();

    // Create a hierarchy from your data
    const root = d3
      .hierarchy({ children: data })
      .sum((d: any) => d.value);

    // Use d3.pack to create the packed layout
    const pack = d3
      .pack()
      .size([width, height])
      .padding(3);

    const packedData = pack(root).leaves();

    // Add a group element for zooming
    const g = svg.append("g");

    // Create circles for each bubble
    g.selectAll("circle")
      .data(packedData)
      .join("circle")
      .attr("cx", (d: d3.HierarchyCircularNode<any>) => d.x)
      .attr("cy", (d: d3.HierarchyCircularNode<any>) => d.y)
      .attr("r", (d: d3.HierarchyCircularNode<any>) => d.r)
      .attr("fill", (d, i) => d3.schemeCategory10[i % 10]);

    // Add labels to the bubbles
    g.selectAll("text")
      .data(packedData)
      .join("text")
      .attr("x", (d: d3.HierarchyCircularNode<any>) => d.x)
      .attr("y", (d: d3.HierarchyCircularNode<any>) => d.y)
      .attr("text-anchor", "middle")
      .attr("dy", "0.3em")
      .style("font-size", "10px")
      .style("fill", "#fff")
      .text((d: d3.HierarchyCircularNode<any>) => d.data.name);

    // Add zoom behavior
    svg.call(
      d3.zoom().scaleExtent([1, 8]).on("zoom", (event) => {
        g.attr("transform", event.transform);
      })
    );
  }, [data]);

  return <svg ref={chartRef} width="100%" height="400" />;
}

export default PackedBubbleChart;*/
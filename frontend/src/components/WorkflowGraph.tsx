import React from 'react';
import type { GraphEdge, GraphNode } from '../types';

interface WorkflowGraphProps {
  nodes: GraphNode[];
  edges: GraphEdge[];
  activeNodeId?: string | null;
}

export const WorkflowGraph: React.FC<WorkflowGraphProps> = ({ nodes, edges, activeNodeId }) => {
  if (nodes.length === 0) {
    return null;
  }

  const width = Math.max(540, nodes.length * 165);
  const height = 148;
  const positions = new Map<string, { x: number; y: number }>();

  nodes.forEach((node, idx) => {
    positions.set(node.id, {
      x: 82 + idx * 155,
      y: idx % 2 === 0 ? 58 : 88,
    });
  });

  return (
    <div style={{ overflowX: 'auto' }}>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        width="100%"
        height="148"
        role="img"
        aria-label="Agent workflow graph topology"
        style={{ background: '#090e1a', borderRadius: 8, border: '1px solid #263248' }}
      >
        <defs>
          <marker
            id="pf-arrow"
            markerWidth="8"
            markerHeight="8"
            refX="7"
            refY="3.5"
            orient="auto"
          >
            <polygon points="0 0, 8 3.5, 0 7" fill="#38bdf8" />
          </marker>
        </defs>

        {edges.map((edge, idx) => {
          const src = positions.get(edge.source);
          const tgt = positions.get(edge.target);
          if (!src || !tgt) {
            return null;
          }
          const midX = (src.x + tgt.x) / 2;
          const isBackEdge = tgt.x <= src.x;
          const curveY = isBackEdge ? 18 : (src.y + tgt.y) / 2 - 12;
          return (
            <g key={`edge-${idx}-${edge.source}-${edge.target}-${edge.action}`}>
              <path
                d={`M ${src.x} ${src.y} Q ${midX} ${curveY} ${tgt.x} ${tgt.y}`}
                fill="none"
                stroke={isBackEdge ? '#f59e0b' : '#38bdf8'}
                strokeWidth="1.8"
                strokeDasharray={isBackEdge ? '4 3' : undefined}
                markerEnd="url(#pf-arrow)"
              />
              <text
                x={midX}
                y={isBackEdge ? 14 : curveY - 3}
                fill="#94a3b8"
                fontSize="10"
                textAnchor="middle"
              >
                {edge.action}
              </text>
            </g>
          );
        })}

        {nodes.map((node, idx) => {
          const pos = positions.get(node.id);
          if (!pos) {
            return null;
          }
          const isActive = activeNodeId === node.id;
          return (
            <g key={`node-${idx}-${node.id}`}>
              <rect
                x={pos.x - 62}
                y={pos.y - 20}
                width="124"
                height="40"
                rx="8"
                fill={isActive ? '#0c4a6e' : '#111827'}
                stroke={isActive ? '#38bdf8' : '#334155'}
                strokeWidth={isActive ? '2' : '1.4'}
              />
              <text
                x={pos.x}
                y={pos.y + 4}
                fill="#f8fafc"
                fontSize="11.5"
                fontWeight="600"
                textAnchor="middle"
              >
                {node.label}
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
};

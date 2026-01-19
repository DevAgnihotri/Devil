import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import numpy as np

class ThreatVisualizer:
    @staticmethod
    def create_timeline(data):
        """Create interactive threat timeline"""
        # Convert timestamp strings to datetime
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        data = data.sort_values('timestamp')
        
        # Create hourly aggregation
        hourly_counts = data.groupby(
            data['timestamp'].dt.floor('H')
        ).size().reset_index(name='count')
        
        # Create threat level aggregation
        threat_levels = data.groupby(
            data['timestamp'].dt.floor('H')
        )['Threat Score'].mean().reset_index(name='avg_threat')
        
        # Merge data
        timeline_data = pd.merge(hourly_counts, threat_levels, on='timestamp')
        
        fig = go.Figure()
        
        # Add bar chart for post counts
        fig.add_trace(go.Bar(
            x=timeline_data['timestamp'],
            y=timeline_data['count'],
            name='Posts per hour',
            marker_color='rgba(14, 165, 233, 0.7)',
            yaxis='y'
        ))
        
        # Add line chart for threat levels
        fig.add_trace(go.Scatter(
            x=timeline_data['timestamp'],
            y=timeline_data['avg_threat'],
            name='Avg Threat Score',
            mode='lines+markers',
            line=dict(color='#ef4444', width=3),
            marker=dict(size=8),
            yaxis='y2'
        ))
        
        # Update layout
        fig.update_layout(
            title="📈 Threat Activity Timeline",
            xaxis_title="Time",
            yaxis_title="Number of Posts",
            yaxis2=dict(
                title="Threat Score",
                overlaying='y',
                side='right',
                range=[0, 10]
            ),
            template='plotly_dark',
            hovermode='x unified',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            height=400
        )
        
        return fig
    
    @staticmethod
    def create_network_graph(accounts, connections):
        """Create network visualization of accounts"""
        nodes = []
        edges = []
        
        # Add nodes (accounts)
        for idx, account in enumerate(accounts):
            nodes.append({
                'id': idx,
                'label': account['username'],
                'size': min(account.get('followers_count', 0) / 100, 50),
                'color': '#ef4444' if account.get('threat_level') == 'HIGH' else 
                        '#f59e0b' if account.get('threat_level') == 'MEDIUM' else 
                        '#10b981'
            })
        
        # Add edges (connections)
        for connection in connections:
            edges.append({
                'from': connection['from'],
                'to': connection['to'],
                'value': connection.get('strength', 1)
            })
        
        # Create network graph using Plotly
        edge_x = []
        edge_y = []
        for edge in edges:
            x0, y0 = nodes[edge['from']]['x'], nodes[edge['from']]['y']
            x1, y1 = nodes[edge['to']]['x'], nodes[edge['to']]['y']
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])
        
        edge_trace = go.Scatter(
            x=edge_x, y=edge_y,
            line=dict(width=0.5, color='#888'),
            hoverinfo='none',
            mode='lines'
        )
        
        node_x = [node['x'] for node in nodes]
        node_y = [node['y'] for node in nodes]
        
        node_trace = go.Scatter(
            x=node_x, y=node_y,
            mode='markers+text',
            text=[node['label'] for node in nodes],
            textposition="top center",
            marker=dict(
                size=[node['size'] for node in nodes],
                color=[node['color'] for node in nodes],
                line_width=2
            )
        )
        
        fig = go.Figure(data=[edge_trace, node_trace],
                       layout=go.Layout(
                           title='Network Analysis',
                           showlegend=False,
                           hovermode='closest',
                           template='plotly_dark'
                       ))
        
        return fig
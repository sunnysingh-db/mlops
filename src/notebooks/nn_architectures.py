"""Neural Network Architectures for Tabular Deep Learning."""
import torch
import torch.nn as nn

class TabularMLP(nn.Module):
    """MLP: Linear -> BN -> Activation -> Dropout per layer."""
    def __init__(self, input_dim, output_dim, hidden_layers, dropout=0.3, activation="relu"):
        super().__init__()
        act_fn = {"relu": nn.ReLU, "gelu": nn.GELU, "silu": nn.SiLU}[activation]
        layers = []
        prev = input_dim
        for h in hidden_layers:
            layers.extend([nn.Linear(prev, h), nn.BatchNorm1d(h), act_fn(), nn.Dropout(dropout)])
            prev = h
        layers.append(nn.Linear(prev, output_dim))
        self.net = nn.Sequential(*layers)
    def forward(self, x):
        return self.net(x)

class ResBlock(nn.Module):
    """Residual block with skip connection."""
    def __init__(self, dim, dropout=0.3, activation="relu"):
        super().__init__()
        act_fn = {"relu": nn.ReLU, "gelu": nn.GELU, "silu": nn.SiLU}[activation]
        self.block = nn.Sequential(nn.Linear(dim, dim), nn.BatchNorm1d(dim), act_fn(), nn.Dropout(dropout), nn.Linear(dim, dim), nn.BatchNorm1d(dim))
        self.act = act_fn()
    def forward(self, x):
        return self.act(x + self.block(x))

class TabularResNet(nn.Module):
    """ResNet-style for tabular data."""
    def __init__(self, input_dim, output_dim, hidden_layers, dropout=0.3, activation="relu"):
        super().__init__()
        h = hidden_layers[0] if hidden_layers else 128
        self.input_proj = nn.Linear(input_dim, h)
        self.blocks = nn.Sequential(*[ResBlock(h, dropout, activation) for _ in range(len(hidden_layers))])
        self.output = nn.Linear(h, output_dim)
    def forward(self, x):
        return self.output(self.blocks(self.input_proj(x)))

def get_architecture(name, input_dim, output_dim, config):
    """Factory: tabular_mlp | tabular_resnet | custom."""
    hl = config.get("hidden_layers", [256, 128, 64])
    dr = config.get("dropout", 0.3)
    act = config.get("activation", "relu")
    if name == "tabular_mlp":
        return TabularMLP(input_dim, output_dim, hl, dr, act)
    elif name == "tabular_resnet":
        return TabularResNet(input_dim, output_dim, hl, dr, act)
    else:
        raise ValueError(f"Unknown architecture: {name}")

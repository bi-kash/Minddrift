import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
current_dir = os.path.dirname(__file__)
file_path = os.path.join(current_dir, 'cholesterol_distribution.csv')


df = pd.read_csv(file_path)
fig, ax = plt.subplots(figsize=(14, 8))
ax.bar(df['cholesterol_level'], df['population_perc'], width=4.5, 
       align='edge', color='blue', edgecolor='pink', alpha=0.7)

ax.set_xlabel('Total Cholesterol', fontsize=12, fontweight='bold')
ax.set_ylabel('% of People', fontsize=12, fontweight='bold')
ax.set_title('Distribution of Total Cholesterol Levels of US Males Aged 40-60', 
             fontsize=14, fontweight='bold')


ax.set_xlim(50, 400)
ax.set_xticks(np.arange(50, 401, 15))
ax.grid(True, alpha=0.3, linestyle='--')

level_184 = df[df['cholesterol_level'] == 180]
percent_184 = level_184['population_perc'].values[0] if len(level_184) > 0 else 0

ax.annotate('184 cholesterol level', 
            xy=(184, percent_184),
            xytext=(240, percent_184 + df['population_perc'].max() * 0.3),
            fontsize=11, fontweight='bold', color='red',
            arrowprops=dict(arrowstyle='->', color='red', lw=3)
            )



plt.tight_layout()
plt.show()

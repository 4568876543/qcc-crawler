"""
Sheet1 生成器 - 生成横向宽表格式
每个区县占2列：[数量] [占比]
"""
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows


class Sheet1Generator:
    """生成 Sheet1 格式的横向宽表"""

    def __init__(self, city_name="重庆市"):
        self.city_name = city_name
        self.industry_data = {}  # {行业名称: {区县: 数量}}
        self.industry_order = []  # 保持行业顺序
        self.districts = []  # 区县列表
        self.total_by_district = {}  # 各区县合计
        self.city_total = 0  # 全市总计

    def add_data(self, industry_code, industry_name, district_counts):
        """
        添加行业数据

        Args:
            industry_code: 行业代码
            industry_name: 行业名称
            district_counts: dict {区县: 企业数量}
        """
        # 排除制造业合计行（如果数据中有）
        if industry_code == 'C' or industry_name == '制造业合计':
            # 统计各区县合计
            for district, count in district_counts.items():
                if district not in self.total_by_district:
                    self.total_by_district[district] = 0
                self.total_by_district[district] += count
            return

        self.industry_data[industry_name] = district_counts
        self.industry_order.append((industry_code, industry_name))

        # 累加区县合计
        for district, count in district_counts.items():
            if district not in self.total_by_district:
                self.total_by_district[district] = 0
            self.total_by_district[district] += count

        # 更新区县列表
        for district in district_counts.keys():
            if district not in self.districts:
                self.districts.append(district)

    def finalize(self):
        """完成数据添加后调用，计算全市总计"""
        self.city_total = sum(self.total_by_district.values())

    def generate(self) -> pd.DataFrame:
        """
        生成 DataFrame

        Returns:
            DataFrame with multi-level columns
        """
        if not self.industry_data:
            raise ValueError("没有添加任何行业数据")

        # 按行业代码排序
        self.industry_order.sort(key=lambda x: int(str(x[0])) if str(x[0]).isdigit() else 0)
        self.districts.sort()

        # 计算全市总计（各区县合计之和）
        self.city_total = sum(self.total_by_district.values())

        # 构建数据
        data = []
        for industry_code, industry_name in self.industry_order:
            row = {'行业分类（31大类）': industry_name}
            counts = self.industry_data[industry_name]

            # 全市合计
            city_sum = sum(counts.values())
            row[self.city_name] = city_sum
            row['占比'] = city_sum / self.city_total if self.city_total > 0 else 0

            # 各区县
            for district in self.districts:
                count = counts.get(district, 0)
                row[f'{district}'] = count
                row[f'{district}.占比'] = count / self.city_total if self.city_total > 0 else 0

            data.append(row)

        # 创建 DataFrame
        df = pd.DataFrame(data)

        # 创建多级列名
        first_level = ['行业分类（31大类）', self.city_name, '占比'] + \
                      [d for d in self.districts for _ in range(2)]

        # 由于 pandas 不支持直接的多级列名，我们创建平铺的列名
        columns = ['行业分类（31大类）', self.city_name, '占比']
        for district in self.districts:
            columns.extend([district, district])

        df.columns = columns

        return df

    def to_excel(self, output_path: str, include_total_row=True):
        """
        直接输出为 Excel 文件

        Args:
            output_path: 输出文件路径
            include_total_row: 是否包含合计行
        """
        df = self.generate()

        wb = Workbook()
        ws = wb.active
        ws.title = "Sheet1"

        # 样式定义
        header_font = Font(bold=True, size=11, color="FFFFFF")
        header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
        total_fill = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")
        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        center_align = Alignment(horizontal='center', vertical='center')

        # 第一行：标题
        last_col = len(df.columns)
        ws.cell(row=1, column=1, value=f"{self.city_name}制造业企业数量统计表")
        ws.cell(row=1, column=1).font = Font(bold=True, size=14)
        ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=min(last_col, 10))

        # 第二行：区县名称（每个区县占2列）
        ws.cell(row=2, column=1, value="行业分类（31大类）")
        ws.cell(row=2, column=1).font = header_font
        ws.cell(row=2, column=1).fill = header_fill
        ws.cell(row=2, column=1).border = thin_border

        ws.cell(row=2, column=2, value=self.city_name)
        ws.cell(row=2, column=2).font = header_font
        ws.cell(row=2, column=2).fill = header_fill
        ws.cell(row=2, column=2).border = thin_border
        ws.merge_cells(start_row=2, start_column=2, end_row=2, end_column=3)

        col_idx = 4
        for district in self.districts:
            ws.cell(row=2, column=col_idx, value=district)
            ws.cell(row=2, column=col_idx).font = header_font
            ws.cell(row=2, column=col_idx).fill = header_fill
            ws.cell(row=2, column=col_idx).border = thin_border
            ws.cell(row=2, column=col_idx).alignment = center_align
            ws.merge_cells(start_row=2, start_column=col_idx, end_row=2, end_column=col_idx + 1)
            col_idx += 2

        # 第三行：数量/占比
        ws.cell(row=3, column=1, value="")
        ws.cell(row=3, column=2, value="数量")
        ws.cell(row=3, column=2).font = Font(bold=True, size=10)
        ws.cell(row=3, column=2).fill = PatternFill(start_color="B4C7E7", end_color="B4C7E7", fill_type="solid")
        ws.cell(row=3, column=3, value="占比")
        ws.cell(row=3, column=3).font = Font(bold=True, size=10)
        ws.cell(row=3, column=3).fill = PatternFill(start_color="B4C7E7", end_color="B4C7E7", fill_type="solid")

        col_idx = 4
        for _ in self.districts:
            ws.cell(row=3, column=col_idx, value="数量")
            ws.cell(row=3, column=col_idx).font = Font(bold=True, size=10)
            ws.cell(row=3, column=col_idx).fill = PatternFill(start_color="B4C7E7", end_color="B4C7E7", fill_type="solid")
            ws.cell(row=3, column=col_idx + 1, value="占比")
            ws.cell(row=3, column=col_idx + 1).font = Font(bold=True, size=10)
            ws.cell(row=3, column=col_idx + 1).fill = PatternFill(start_color="B4C7E7", end_color="B4C7E7", fill_type="solid")
            col_idx += 2

        # 数据行
        for row_idx, row_data in df.iterrows():
            excel_row = row_idx + 4  # 从第4行开始

            col_idx = 1
            for col_idx, col_name in enumerate(df.columns, 1):
                value = row_data[col_name]

                # 格式设置
                cell = ws.cell(row=excel_row, column=col_idx, value=value)
                cell.border = thin_border

                if col_idx == 1:  # 行业名称
                    cell.font = Font(bold=True)
                elif '占比' in str(col_name):  # 占比列
                    if isinstance(value, (int, float)):
                        cell.number_format = '0.00%'
                else:  # 数量列
                    if isinstance(value, (int, float)):
                        cell.number_format = '#,##0'

        # 合计行
        if include_total_row:
            total_row = len(df) + 4
            ws.cell(row=total_row, column=1, value="合计").font = Font(bold=True)
            ws.cell(row=total_row, column=1).fill = total_fill
            ws.cell(row=total_row, column=1).border = thin_border

            ws.cell(row=total_row, column=2, value=self.city_total)
            ws.cell(row=total_row, column=2).font = Font(bold=True)
            ws.cell(row=total_row, column=2).fill = total_fill
            ws.cell(row=total_row, column=2).border = thin_border
            ws.cell(row=total_row, column=2).number_format = '#,##0'

            ws.cell(row=total_row, column=3, value=1.0)
            ws.cell(row=total_row, column=3).font = Font(bold=True)
            ws.cell(row=total_row, column=3).fill = total_fill
            ws.cell(row=total_row, column=3).border = thin_border
            ws.cell(row=total_row, column=3).number_format = '0.00%'

            col_idx = 4
            for district in self.districts:
                total = self.total_by_district.get(district, 0)
                ws.cell(row=total_row, column=col_idx, value=total)
                ws.cell(row=total_row, column=col_idx).font = Font(bold=True)
                ws.cell(row=total_row, column=col_idx).fill = total_fill
                ws.cell(row=total_row, column=col_idx).border = thin_border
                ws.cell(row=total_row, column=col_idx).number_format = '#,##0'

                ratio = total / self.city_total if self.city_total > 0 else 0
                ws.cell(row=total_row, column=col_idx + 1, value=ratio)
                ws.cell(row=total_row, column=col_idx + 1).font = Font(bold=True)
                ws.cell(row=total_row, column=col_idx + 1).fill = total_fill
                ws.cell(row=total_row, column=col_idx + 1).border = thin_border
                ws.cell(row=total_row, column=col_idx + 1).number_format = '0.00%'

                col_idx += 2

        # 设置列宽
        ws.column_dimensions['A'].width = 25
        for i, district in enumerate(self.districts):
            col_letter = chr(65 + 4 + i * 2)  # D, F, H, ...
            ws.column_dimensions[col_letter].width = 12
            ws.column_dimensions[chr(65 + 4 + i * 2 + 1)].width = 8

        wb.save(output_path)
        print(f"[Sheet1] 已保存到: {output_path}")

        return output_path


if __name__ == "__main__":
    # 测试代码
    generator = Sheet1Generator("重庆市")

    # 模拟数据
    generator.add_data('C', '制造业合计', {
        '万州区': 13046, '涪陵区': 11142, '渝中区': 5038
    })

    generator.add_data('13', '农副食品加工业', {
        '万州区': 1688, '涪陵区': 2745, '渝中区': 265
    })

    generator.add_data('14', '食品制造业', {
        '万州区': 1279, '涪陵区': 843, '渝中区': 361
    })

    df = generator.generate()
    print(df)

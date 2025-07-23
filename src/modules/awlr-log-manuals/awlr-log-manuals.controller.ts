import {
  Controller,
  Get,
  Post,
  Body,
  Param,
  Patch,
  Delete,
} from '@nestjs/common';
import { AwlrLogManualsService } from './awlr-log-manuals.service';
import { CreateAwlrLogManualDto } from './dto/create-awlr-log-manual.dto';
import { UpdateAwlrLogManualDto } from './dto/update-awlr-log-manual.dto';

@Controller('awlr-log-manuals')
export class AwlrLogManualsController {
  constructor(private readonly service: AwlrLogManualsService) {}

  @Post()
  async create(@Body() dto: CreateAwlrLogManualDto) {
    const created = await this.service.create(dto);
    return {
      message: 'Log successfully created',
      data: created,
    };
  }

  @Get()
  async findAll() {
    const data = await this.service.findAll();
    return {
      message: 'All data fetched successfully',
      data,
    };
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    const data = await this.service.findOne(+id);
    return {
      message: `Data fetched for ID ${id}`,
      data,
    };
  }

  @Patch(':id')
  async update(@Param('id') id: string, @Body() dto: UpdateAwlrLogManualDto) {
    const data = await this.service.update(+id, dto);
    return {
      message: `Data successfully updated for ID ${id}`,
      data,
    };
  }

  @Delete(':id')
  async remove(@Param('id') id: string) {
    const data = await this.service.remove(+id);
    return {
      message: `Data successfully deleted for ID ${id}`,
      data,
    };
  }
}

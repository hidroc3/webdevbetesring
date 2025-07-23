import {
  Controller,
  Get,
  Post,
  Body,
  Param,
  Patch,
  Delete,
} from '@nestjs/common';
import { AwsLogManualsService } from './aws-log-manuals.service';
import { CreateAwsLogManualDto } from './dto/create-aws-log-manual.dto';
import { UpdateAwsLogManualDto } from './dto/update-aws-log-manual.dto';

@Controller('aws-log-manuals')
export class AwsLogManualsController {
  constructor(private readonly service: AwsLogManualsService) {}

  @Post()
  async create(@Body() dto: CreateAwsLogManualDto) {
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
  async update(@Param('id') id: string, @Body() dto: UpdateAwsLogManualDto) {
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

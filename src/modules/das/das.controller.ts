import {
  Controller,
  Get,
  Post,
  Body,
  Param,
  Patch,
  Delete,
} from '@nestjs/common';
import { DasService } from './das.service';
import { CreateDasDto } from './dto/create-das.dto';
import { UpdateDasDto } from './dto/update-das.dto';

@Controller('das')
export class DasController {
  constructor(private readonly service: DasService) {}

  @Post()
  async create(@Body() dto: CreateDasDto) {
    const data = await this.service.create(dto);
    return {
      message: 'DAS successfully created',
      data,
    };
  }

  @Get()
  async findAll() {
    const data = await this.service.findAll();
    return {
      message: 'All DAS entries fetched successfully',
      data,
    };
  }

  @Get(':id')
  async findOne(@Param('id') id: string) {
    const data = await this.service.findOne(+id);
    return {
      message: `DAS entry fetched for ID ${id}`,
      data,
    };
  }

  @Patch(':id')
  async update(@Param('id') id: string, @Body() dto: UpdateDasDto) {
    const data = await this.service.update(+id, dto);
    return {
      message: `DAS entry updated for ID ${id}`,
      data,
    };
  }

  @Delete(':id')
  async remove(@Param('id') id: string) {
    const data = await this.service.remove(+id);
    return {
      message: `DAS entry deleted for ID ${id}`,
      data,
    };
  }
}

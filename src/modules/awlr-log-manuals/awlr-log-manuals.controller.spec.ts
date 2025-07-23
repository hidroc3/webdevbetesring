import { Test, TestingModule } from '@nestjs/testing';
import { AwlrLogManualsController } from './awlr-log-manuals.controller';

describe('AwlrLogManualsController', () => {
  let controller: AwlrLogManualsController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [AwlrLogManualsController],
    }).compile();

    controller = module.get<AwlrLogManualsController>(AwlrLogManualsController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
